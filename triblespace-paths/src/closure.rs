use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use triblespace_core::inline::RawInline;

use crate::automaton::StateId;
use crate::index::{BuildStats, ProductPoint};

/// A reflexive, transitively closed relation over a fixed product carrier.
///
/// This ablation retains the direct product arcs, condenses their SCCs once,
/// and stores reachability between components as dense bitsets. Point rows are
/// projected lazily from that representation instead of being copied into
/// sparse row and column trees.
#[derive(Clone, Debug)]
pub(crate) struct Closure {
    points: Vec<ProductPoint>,
    position: BTreeMap<ProductPoint, usize>,
    direct_pairs: Vec<(ProductPoint, ProductPoint)>,
    component_of: Vec<usize>,
    component_members: Vec<Vec<usize>>,
    component_reach: Vec<Vec<u64>>,
    pair_count: usize,
}

impl Closure {
    pub(crate) fn from_pairs(
        vertices: &[RawInline],
        state_count: StateId,
        pairs: impl IntoIterator<Item = (ProductPoint, ProductPoint)>,
        stats: &mut BuildStats,
    ) -> Self {
        let setup_started = Instant::now();
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

        let mut direct_pairs = BTreeSet::new();
        for (source, target) in pairs {
            stats.seed_pairs_considered += 1;
            if source != target {
                direct_pairs.insert((source, target));
            }
        }
        let direct_pairs = direct_pairs.into_iter().collect::<Vec<_>>();
        stats.effective_insertions = direct_pairs.len();

        let mut adjacency = vec![Vec::new(); point_count];
        for &(source, target) in &direct_pairs {
            adjacency[position[&source]].push(position[&target]);
        }
        stats.batch_setup_ns = setup_started.elapsed().as_nanos();

        let scc_started = Instant::now();
        let (component_of, component_members) = strongly_connected_components(&adjacency);
        let component_count = component_members.len();
        let mut component_edges = vec![BTreeSet::new(); component_count];
        for (source, targets) in adjacency.iter().enumerate() {
            let source_component = component_of[source];
            for &target in targets {
                let target_component = component_of[target];
                if source_component != target_component {
                    component_edges[source_component].insert(target_component);
                }
            }
        }
        let component_edges = component_edges
            .into_iter()
            .map(BTreeSet::into_iter)
            .map(Iterator::collect::<Vec<_>>)
            .collect::<Vec<_>>();
        let topological = topological_order(&component_edges);
        stats.batch_components = component_count;
        stats.batch_condensation_edges = component_edges.iter().map(Vec::len).sum();
        stats.batch_scc_ns = scc_started.elapsed().as_nanos();

        let propagation_started = Instant::now();
        let word_count = component_count.div_ceil(u64::BITS as usize);
        let mut component_reach = vec![vec![0u64; word_count]; component_count];
        for (component, row) in component_reach.iter_mut().enumerate() {
            set_bit(row, component);
        }
        for &component in topological.iter().rev() {
            for &successor in &component_edges[component] {
                union_rows(&mut component_reach, component, successor);
            }
        }
        stats.batch_bitset_words = component_count.saturating_mul(word_count);
        stats.batch_word_ors = stats.batch_condensation_edges.saturating_mul(word_count);
        stats.batch_propagation_ns = propagation_started.elapsed().as_nanos();

        let pair_count_started = Instant::now();
        let reachable_members = component_reach
            .iter()
            .map(|row| {
                set_bits(row)
                    .map(|component| component_members[component].len())
                    .sum::<usize>()
            })
            .collect::<Vec<_>>();
        let pair_count = component_members
            .iter()
            .enumerate()
            .map(|(component, members)| members.len().saturating_mul(reachable_members[component]))
            .sum();
        stats.batch_pair_count_ns = pair_count_started.elapsed().as_nanos();
        record_compatibility_work(stats, point_count, pair_count);

        Self {
            points,
            position,
            direct_pairs,
            component_of,
            component_members,
            component_reach,
            pair_count,
        }
    }

    pub(crate) fn direct_pairs(&self) -> impl Iterator<Item = (ProductPoint, ProductPoint)> + '_ {
        self.direct_pairs.iter().copied()
    }

    pub(crate) fn reaches(&self, source: ProductPoint, target: ProductPoint) -> bool {
        let Some(&source) = self.position.get(&source) else {
            return false;
        };
        let Some(&target) = self.position.get(&target) else {
            return false;
        };
        self.reaches_index(source, target)
    }

    pub(crate) fn reaches_index(&self, source: usize, target: usize) -> bool {
        bit_is_set(
            &self.component_reach[self.component_of[source]],
            self.component_of[target],
        )
    }

    pub(crate) fn row(&self, source: usize) -> impl Iterator<Item = usize> + '_ {
        let reach = &self.component_reach[self.component_of[source]];
        // Component numbering is an SCC-algorithm detail and can interleave
        // globally ordered points. Filter canonical point indices so callers
        // retain the baseline's ascending row order.
        (0..self.points.len()).filter(move |&target| bit_is_set(reach, self.component_of[target]))
    }

    /// Reachable point indices in SCC order.
    ///
    /// This deliberately makes no ordering promise: it is the cheap internal
    /// path for constructing a bitset projection, whose result establishes
    /// its own canonical order. Public product rows continue to use `row`.
    pub(crate) fn reachable_indices_unordered(
        &self,
        source: usize,
    ) -> impl Iterator<Item = usize> + '_ {
        let reach = &self.component_reach[self.component_of[source]];
        set_bits(reach).flat_map(|component| self.component_members[component].iter().copied())
    }

    pub(crate) fn pair_count(&self) -> usize {
        self.pair_count
    }

    pub(crate) fn pairs(&self) -> impl Iterator<Item = (ProductPoint, ProductPoint)> + '_ {
        (0..self.points.len()).flat_map(move |source| {
            self.row(source)
                .map(move |target| (self.points[source], self.points[target]))
        })
    }
}

fn strongly_connected_components(adjacency: &[Vec<usize>]) -> (Vec<usize>, Vec<Vec<usize>>) {
    let mut reverse = vec![Vec::new(); adjacency.len()];
    for (source, targets) in adjacency.iter().enumerate() {
        for &target in targets {
            reverse[target].push(source);
        }
    }

    let mut seen = vec![false; adjacency.len()];
    let mut postorder = Vec::with_capacity(adjacency.len());
    for root in 0..adjacency.len() {
        if seen[root] {
            continue;
        }
        seen[root] = true;
        let mut stack = vec![(root, 0usize)];
        while let Some((node, next_edge)) = stack.last_mut() {
            if *next_edge < adjacency[*node].len() {
                let target = adjacency[*node][*next_edge];
                *next_edge += 1;
                if !seen[target] {
                    seen[target] = true;
                    stack.push((target, 0));
                }
            } else {
                postorder.push(*node);
                stack.pop();
            }
        }
    }

    let mut component_of = vec![usize::MAX; adjacency.len()];
    let mut components = Vec::new();
    for &root in postorder.iter().rev() {
        if component_of[root] != usize::MAX {
            continue;
        }
        let component = components.len();
        let mut members = Vec::new();
        let mut stack = vec![root];
        component_of[root] = component;
        while let Some(node) = stack.pop() {
            members.push(node);
            for &predecessor in &reverse[node] {
                if component_of[predecessor] == usize::MAX {
                    component_of[predecessor] = component;
                    stack.push(predecessor);
                }
            }
        }
        members.sort_unstable();
        components.push(members);
    }
    (component_of, components)
}

fn topological_order(adjacency: &[Vec<usize>]) -> Vec<usize> {
    let mut indegree = vec![0usize; adjacency.len()];
    for targets in adjacency {
        for &target in targets {
            indegree[target] += 1;
        }
    }
    let mut ready = indegree
        .iter()
        .enumerate()
        .filter(|(_, degree)| **degree == 0)
        .map(|(component, _)| component)
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(adjacency.len());
    while let Some(component) = ready.pop_first() {
        order.push(component);
        for &target in &adjacency[component] {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.insert(target);
            }
        }
    }
    assert_eq!(order.len(), adjacency.len(), "SCC condensation is a DAG");
    order
}

fn union_rows(rows: &mut [Vec<u64>], target: usize, source: usize) {
    if target < source {
        let (left, right) = rows.split_at_mut(source);
        for (target_word, source_word) in left[target].iter_mut().zip(&right[0]) {
            *target_word |= source_word;
        }
    } else {
        let (left, right) = rows.split_at_mut(target);
        for (target_word, source_word) in right[0].iter_mut().zip(&left[source]) {
            *target_word |= source_word;
        }
    }
}

fn set_bit(words: &mut [u64], bit: usize) {
    words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
}

fn bit_is_set(words: &[u64], bit: usize) -> bool {
    words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0
}

fn set_bits(words: &[u64]) -> impl Iterator<Item = usize> + '_ {
    words
        .iter()
        .copied()
        .enumerate()
        .flat_map(|(word_index, mut word)| {
            std::iter::from_fn(move || {
                if word == 0 {
                    return None;
                }
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                Some(word_index * u64::BITS as usize + bit)
            })
        })
}

/// Keeps the pre-ablation work-accounting surface internally consistent. The
/// batch kernel performs no rank-one rectangles: direct arcs are charged one
/// cell each and all closure-derived pairs are charged to one aggregate batch.
fn record_compatibility_work(stats: &mut BuildStats, points: usize, pairs: usize) {
    stats.pairs_added = pairs.saturating_sub(points);
    stats.derived_pairs = stats.pairs_added.saturating_sub(stats.effective_insertions);
    if stats.effective_insertions == 0 {
        return;
    }

    let aggregate = stats.derived_pairs.saturating_add(1);
    stats.largest_rectangle = aggregate;
    stats.rectangle_cells_considered = stats.pairs_added;
    stats.rectangle_log2_counts[0] = stats.effective_insertions - 1;
    stats.rectangle_log2_cells[0] = stats.effective_insertions - 1;
    let bucket = aggregate.ilog2() as usize;
    stats.rectangle_log2_counts[bucket] += 1;
    stats.rectangle_log2_cells[bucket] =
        stats.rectangle_log2_cells[bucket].saturating_add(aggregate);
}
