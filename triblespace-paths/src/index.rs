use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::time::Instant;

use triblespace_core::id::RawId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::RawInline;
use triblespace_core::trible::Trible;

use crate::automaton::{Automaton, StateId};
use crate::closure::Closure;

/// Number of power-of-two buckets needed to classify every nonzero `usize`
/// rectangle area.
pub const RECTANGLE_LOG2_BUCKETS: usize = usize::BITS as usize;

/// One directed, attribute-labeled graph edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct GraphEdge {
    /// Edge source.
    pub source: RawInline,
    /// Edge label.
    pub attribute: RawId,
    /// Edge target. It may be an ID or any other inline value.
    pub target: RawInline,
}

impl From<&Trible> for GraphEdge {
    fn from(trible: &Trible) -> Self {
        Self {
            source: RawInline::from(*trible.e()),
            attribute: RawId::from(*trible.a()),
            target: trible.v::<UnknownInline>().raw,
        }
    }
}

impl From<Trible> for GraphEdge {
    fn from(trible: Trible) -> Self {
        Self::from(&trible)
    }
}

/// One vertex/state point in the graph-automaton product.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProductPoint {
    /// Graph term.
    pub vertex: RawInline,
    /// Automaton state.
    pub state: StateId,
}

/// Work performed while constructing one exact closure.
///
/// The `rectangle_*` fields on this SCC ablation branch are non-semantic
/// compatibility accounting for the pre-existing measurement harness. They
/// do not describe executed SCC/bitset work and are not a production API
/// proposal; the `batch_*` fields describe the actual ablation kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildStats {
    /// Distinct graph edges supplied to a leaf build.
    pub graph_edges: usize,
    /// Product arcs or child-summary pairs offered to the closure kernel.
    pub seed_pairs_considered: usize,
    /// Distinct non-identity direct product arcs entering the batch kernel.
    pub effective_insertions: usize,
    /// Total non-identity product pairs in the completed closure.
    pub pairs_added: usize,
    /// Closure pairs other than the distinct direct product arcs.
    pub derived_pairs: usize,
    /// Compatibility view of the largest logical batch of closure work.
    ///
    /// The SCC ablation performs no rank-one updates. It charges direct arcs
    /// one cell each and all derived pairs to one aggregate batch so existing
    /// observational consumers remain internally additive.
    pub largest_rectangle: usize,
    /// Compatibility work cells; equal to [`BuildStats::pairs_added`].
    pub rectangle_cells_considered: usize,
    /// Compatibility logical batches by power-of-two work scale.
    pub rectangle_log2_counts: [usize; RECTANGLE_LOG2_BUCKETS],
    /// Compatibility work cells in each logical-batch bucket.
    pub rectangle_log2_cells: [usize; RECTANGLE_LOG2_BUCKETS],
    /// Strongly connected components in the direct product graph.
    pub batch_components: usize,
    /// Edges in the SCC condensation DAG.
    pub batch_condensation_edges: usize,
    /// Words retained by the dense component-reachability matrix.
    pub batch_bitset_words: usize,
    /// Word ORs performed during reverse-topological propagation.
    pub batch_word_ors: usize,
    /// Product-carrier and direct-adjacency setup time.
    pub batch_setup_ns: u128,
    /// SCC and condensation construction time.
    pub batch_scc_ns: u128,
    /// Reverse-topological bitset propagation time.
    pub batch_propagation_ns: u128,
    /// Exact product-pair cardinality scan over component bitsets.
    pub batch_pair_count_ns: u128,
    /// Bytes retained by the canonical forward accepted-endpoint CSR.
    pub accepted_canonical_bytes: usize,
    /// Bytes retained by the derived reverse CSR and domain accelerators.
    pub accepted_accelerator_bytes: usize,
    /// Accepted-endpoint CSR and accelerator construction time.
    pub projection_ns: u128,
}

impl Default for BuildStats {
    fn default() -> Self {
        Self {
            graph_edges: 0,
            seed_pairs_considered: 0,
            effective_insertions: 0,
            pairs_added: 0,
            derived_pairs: 0,
            largest_rectangle: 0,
            rectangle_cells_considered: 0,
            rectangle_log2_counts: [0; RECTANGLE_LOG2_BUCKETS],
            rectangle_log2_cells: [0; RECTANGLE_LOG2_BUCKETS],
            batch_components: 0,
            batch_condensation_edges: 0,
            batch_bitset_words: 0,
            batch_word_ors: 0,
            batch_setup_ns: 0,
            batch_scc_ns: 0,
            batch_propagation_ns: 0,
            batch_pair_count_ns: 0,
            accepted_canonical_bytes: 0,
            accepted_accelerator_bytes: 0,
            projection_ns: 0,
        }
    }
}

/// Size of the retained product relation and its user-visible projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexMetrics {
    /// Graph terms in the zero-hop universe.
    pub vertices: usize,
    /// States in the fixed automaton.
    pub automaton_states: usize,
    /// `vertices × automaton_states` retained product points.
    pub product_points: usize,
    /// Reachable ordered product-point pairs, including identity.
    pub product_pairs: usize,
    /// Distinct accepted graph-term pairs.
    pub accepted_pairs: usize,
}

impl IndexMetrics {
    /// Fraction of the dense product relation occupied by reachable pairs.
    pub fn product_density(self) -> f64 {
        let possible = self.product_points.saturating_mul(self.product_points);
        if possible == 0 {
            0.0
        } else {
            self.product_pairs as f64 / possible as f64
        }
    }
}

/// Exact in-memory relation for one fixed automaton.
///
/// Every product point is retained. This makes arbitrary future segment merges
/// exact and turns state explosion into a directly measurable property rather
/// than hiding it behind an unsound boundary heuristic.
#[derive(Clone, Debug)]
pub struct PathIndex {
    automaton: Automaton,
    vertices: Vec<RawInline>,
    closure: Closure,
    accepted: AcceptedRelation,
    build_stats: BuildStats,
}

/// Sorted accepted endpoint fibers over `PathIndex::vertices`.
///
/// `forward` is the sole endpoint denotation. `reverse` and the three domains
/// are explicitly derived accelerators over the same pairs; none can affect
/// merge semantics.
#[derive(Clone, Debug)]
struct AcceptedRelation {
    forward: Csr,
    reverse: Csr,
    starts: Vec<u32>,
    ends: Vec<u32>,
    diagonal: Vec<u32>,
}

/// Compressed sparse rows with sorted, duplicate-free `u32` ordinals.
#[derive(Clone, Debug)]
struct Csr {
    offsets: Vec<usize>,
    ordinals: Vec<u32>,
}

impl AcceptedRelation {
    fn from_closure(automaton: &Automaton, vertex_count: usize, closure: &Closure) -> Self {
        let state_count = automaton.state_count() as usize;
        let initial = automaton
            .initial_states()
            .map(|state| state as usize)
            .collect::<Vec<_>>();
        let mut offsets = Vec::with_capacity(vertex_count + 1);
        let mut ordinals = Vec::new();
        let mut row = vec![0u64; vertex_count.div_ceil(u64::BITS as usize)];
        offsets.push(0);

        for source in 0..vertex_count {
            for &initial_state in &initial {
                for target in
                    closure.reachable_indices_unordered(source * state_count + initial_state)
                {
                    if !automaton.is_accepting((target % state_count) as StateId) {
                        continue;
                    }
                    let target = target / state_count;
                    row[target / u64::BITS as usize] |= 1u64 << (target % u64::BITS as usize);
                }
            }
            for (word_index, word) in row.iter_mut().enumerate() {
                while *word != 0 {
                    let bit = word.trailing_zeros() as usize;
                    *word &= *word - 1;
                    let target = word_index * u64::BITS as usize + bit;
                    ordinals.push(u32::try_from(target).expect("path vertex ordinal exceeds u32"));
                }
            }
            offsets.push(ordinals.len());
        }

        let forward = Csr { offsets, ordinals };
        let reverse = forward.transpose(vertex_count);
        let starts = forward.nonempty_rows();
        let ends = reverse.nonempty_rows();
        let diagonal = (0..vertex_count)
            .filter(|&vertex| {
                let vertex = u32::try_from(vertex).expect("path vertex ordinal exceeds u32");
                forward.row(vertex as usize).binary_search(&vertex).is_ok()
            })
            .map(|vertex| u32::try_from(vertex).expect("path vertex ordinal exceeds u32"))
            .collect();

        Self {
            forward,
            reverse,
            starts,
            ends,
            diagonal,
        }
    }

    fn contains(&self, source: usize, target: usize) -> bool {
        let Ok(target) = u32::try_from(target) else {
            return false;
        };
        self.forward.row(source).binary_search(&target).is_ok()
    }
}

impl Csr {
    fn row(&self, row: usize) -> &[u32] {
        &self.ordinals[self.offsets[row]..self.offsets[row + 1]]
    }

    fn transpose(&self, row_count: usize) -> Self {
        let mut counts = vec![0usize; row_count];
        for &target in &self.ordinals {
            counts[target as usize] += 1;
        }

        let mut offsets = Vec::with_capacity(row_count + 1);
        offsets.push(0);
        for count in counts {
            offsets.push(offsets.last().copied().unwrap_or(0) + count);
        }

        let mut next = offsets[..row_count].to_vec();
        let mut ordinals = vec![0u32; self.ordinals.len()];
        for source in 0..row_count {
            let source = u32::try_from(source).expect("path vertex ordinal exceeds u32");
            for &target in self.row(source as usize) {
                let slot = &mut next[target as usize];
                ordinals[*slot] = source;
                *slot += 1;
            }
        }

        Self { offsets, ordinals }
    }

    fn nonempty_rows(&self) -> Vec<u32> {
        self.offsets
            .windows(2)
            .enumerate()
            .filter(|(_, range)| range[0] != range[1])
            .map(|(row, _)| u32::try_from(row).expect("path vertex ordinal exceeds u32"))
            .collect()
    }

    fn storage_bytes(&self) -> usize {
        self.offsets
            .len()
            .saturating_mul(std::mem::size_of::<usize>())
            .saturating_add(
                self.ordinals
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }
}

/// Segment summaries cannot be combined under these conditions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeError {
    /// `merge_all` was called without an index from which to obtain the fixed
    /// automaton.
    EmptyInput,
    /// Every merged segment must use the same canonical automaton.
    DifferentAutomata,
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "cannot merge an empty index list"),
            Self::DifferentAutomata => write!(f, "path indexes use different automata"),
        }
    }
}

impl Error for MergeError {}

impl PathIndex {
    /// Builds an exact leaf summary from a SET of graph edges.
    pub fn from_edges(automaton: Automaton, edges: impl IntoIterator<Item = GraphEdge>) -> Self {
        let edges = edges.into_iter().collect::<BTreeSet<_>>();
        let vertices = edges
            .iter()
            .flat_map(|edge| [edge.source, edge.target])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut build_stats = BuildStats {
            graph_edges: edges.len(),
            ..BuildStats::default()
        };
        let mut product_pairs = Vec::new();
        for edge in edges {
            for transition in automaton.transitions() {
                if !transition.step.matches(&edge.attribute) {
                    continue;
                }
                let (source, target) = if transition.step.is_reverse() {
                    (edge.target, edge.source)
                } else {
                    (edge.source, edge.target)
                };
                product_pairs.push((
                    ProductPoint {
                        vertex: source,
                        state: transition.from,
                    },
                    ProductPoint {
                        vertex: target,
                        state: transition.to,
                    },
                ));
            }
        }
        let closure = Closure::from_pairs(
            &vertices,
            automaton.state_count(),
            product_pairs,
            &mut build_stats,
        );

        Self::finish(automaton, vertices, closure, build_stats)
    }

    /// Builds a leaf summary directly from tribles.
    pub fn from_tribles<'a>(
        automaton: Automaton,
        tribles: impl IntoIterator<Item = &'a Trible>,
    ) -> Self {
        Self::from_edges(automaton, tribles.into_iter().map(GraphEdge::from))
    }

    /// Closes the union of two complete product relations.
    ///
    /// This is stronger than unioning their accepted endpoint pairs: it
    /// recovers paths that alternate between the two segments any number of
    /// times.
    pub fn merge(&self, other: &Self) -> Result<Self, MergeError> {
        Self::merge_all([self, other])
    }

    /// Closes the union of all live segment relations in one snapshot.
    pub fn merge_all<'a>(indexes: impl IntoIterator<Item = &'a Self>) -> Result<Self, MergeError> {
        let indexes = indexes.into_iter().collect::<Vec<_>>();
        let Some(first) = indexes.first() else {
            return Err(MergeError::EmptyInput);
        };
        let automaton = first.automaton.clone();
        if indexes.iter().any(|index| index.automaton != automaton) {
            return Err(MergeError::DifferentAutomata);
        }

        let vertices = indexes
            .iter()
            .flat_map(|index| index.vertices.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut build_stats = BuildStats::default();

        // Retaining direct product arcs makes the semilattice union
        // constructional: recompute one closure over their canonical union
        // instead of feeding every transitive child pair back as adjacency.
        let seeds = indexes
            .iter()
            .flat_map(|index| index.closure.direct_pairs())
            .collect::<BTreeSet<_>>();
        let closure =
            Closure::from_pairs(&vertices, automaton.state_count(), seeds, &mut build_stats);

        Ok(Self::finish(automaton, vertices, closure, build_stats))
    }

    fn finish(
        automaton: Automaton,
        vertices: Vec<RawInline>,
        closure: Closure,
        mut build_stats: BuildStats,
    ) -> Self {
        let projection_started = Instant::now();
        let accepted = AcceptedRelation::from_closure(&automaton, vertices.len(), &closure);
        build_stats.accepted_canonical_bytes = accepted.forward.storage_bytes();
        build_stats.accepted_accelerator_bytes = accepted
            .reverse
            .storage_bytes()
            .saturating_add(
                accepted
                    .starts
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                accepted
                    .ends
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                accepted
                    .diagonal
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            );
        build_stats.projection_ns = projection_started.elapsed().as_nanos();

        Self {
            automaton,
            vertices,
            closure,
            accepted,
            build_stats,
        }
    }

    /// Fixed automaton defining this relation.
    pub fn automaton(&self) -> &Automaton {
        &self.automaton
    }

    /// Whether the automaton accepts a path from `source` to `target`.
    pub fn contains(&self, source: &RawInline, target: &RawInline) -> bool {
        let Ok(source) = self.vertices.binary_search(source) else {
            return false;
        };
        let Ok(target) = self.vertices.binary_search(target) else {
            return false;
        };
        self.accepted.contains(source, target)
    }

    /// Sorted, duplicate-free accepted endpoint pairs.
    pub fn accepted_pairs(&self) -> impl Iterator<Item = (RawInline, RawInline)> + '_ {
        self.vertices
            .iter()
            .copied()
            .enumerate()
            .flat_map(move |(source_index, source)| {
                self.accepted
                    .forward
                    .row(source_index)
                    .iter()
                    .map(move |&target_index| (source, self.vertices[target_index as usize]))
            })
    }

    /// Sorted, duplicate-free accepted targets for one source.
    pub fn reachable_from<'a>(
        &'a self,
        source: &RawInline,
    ) -> impl Iterator<Item = RawInline> + 'a {
        self.values(self.forward_ordinals(source))
    }

    /// Sorted, duplicate-free accepted sources for one target.
    pub fn reaching<'a>(&'a self, target: &RawInline) -> impl Iterator<Item = RawInline> + 'a {
        self.values(self.reverse_ordinals(target))
    }

    /// Whether one product point reaches another, including reflexive
    /// identity over the retained carrier.
    pub fn product_reaches(&self, source: ProductPoint, target: ProductPoint) -> bool {
        self.closure.reaches(source, target)
    }

    /// Complete product relation, including identity, sorted by source and
    /// then target product point.
    pub fn product_pairs(&self) -> impl Iterator<Item = (ProductPoint, ProductPoint)> + '_ {
        self.closure.pairs()
    }

    /// Construction work for this leaf or merge.
    pub fn build_stats(&self) -> BuildStats {
        self.build_stats
    }

    /// Current state size before any succinct encoding.
    pub fn metrics(&self) -> IndexMetrics {
        IndexMetrics {
            vertices: self.vertices.len(),
            automaton_states: self.automaton.state_count() as usize,
            product_points: self.vertices.len() * self.automaton.state_count() as usize,
            product_pairs: self.closure.pair_count(),
            accepted_pairs: self.accepted.forward.ordinals.len(),
        }
    }

    pub(crate) fn forward_ordinals(&self, source: &RawInline) -> &[u32] {
        self.vertices
            .binary_search(source)
            .ok()
            .map(|source| self.accepted.forward.row(source))
            .unwrap_or(&[])
    }

    pub(crate) fn reverse_ordinals(&self, target: &RawInline) -> &[u32] {
        self.vertices
            .binary_search(target)
            .ok()
            .map(|target| self.accepted.reverse.row(target))
            .unwrap_or(&[])
    }

    pub(crate) fn starts_ordinals(&self) -> &[u32] {
        &self.accepted.starts
    }

    pub(crate) fn ends_ordinals(&self) -> &[u32] {
        &self.accepted.ends
    }

    pub(crate) fn diagonal_ordinals(&self) -> &[u32] {
        &self.accepted.diagonal
    }

    pub(crate) fn values<'a>(
        &'a self,
        ordinals: &'a [u32],
    ) -> impl Iterator<Item = RawInline> + 'a {
        ordinals
            .iter()
            .map(|&vertex| self.vertices[vertex as usize])
    }

    pub(crate) fn ordinals_contain(&self, ordinals: &[u32], value: &RawInline) -> bool {
        self.vertices
            .binary_search(value)
            .ok()
            .and_then(|vertex| u32::try_from(vertex).ok())
            .is_some_and(|vertex| ordinals.binary_search(&vertex).is_ok())
    }
}
