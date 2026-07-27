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
    /// Words retained by the experimental accepted-endpoint bitrelation.
    pub accepted_bitset_words: usize,
    /// Words retained by its derived transpose and domain accelerators.
    pub accepted_accelerator_words: usize,
    /// Experimental accepted-endpoint bitrelation construction time.
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
            accepted_bitset_words: 0,
            accepted_accelerator_words: 0,
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

/// Row-major accepted endpoint relation over `PathIndex::vertices`.
///
/// `forward` is the sole endpoint denotation. `reverse` and the three domain
/// masks are explicitly derived accelerators over the same bits; none can
/// affect merge semantics.
#[derive(Clone, Debug)]
struct AcceptedRelation {
    row_words: usize,
    forward: Vec<u64>,
    reverse: Vec<u64>,
    starts: Vec<u64>,
    ends: Vec<u64>,
    diagonal: Vec<u64>,
    pair_count: usize,
}

impl AcceptedRelation {
    fn from_closure(automaton: &Automaton, vertex_count: usize, closure: &Closure) -> Self {
        let row_words = vertex_count.div_ceil(u64::BITS as usize);
        let relation_words = vertex_count.saturating_mul(row_words);
        let mut forward = vec![0u64; relation_words];
        let mut reverse = vec![0u64; relation_words];
        let mut starts = vec![0u64; row_words];
        let mut ends = vec![0u64; row_words];
        let mut diagonal = vec![0u64; row_words];
        let mut pair_count = 0usize;
        let state_count = automaton.state_count() as usize;
        let initial = automaton
            .initial_states()
            .map(|state| state as usize)
            .collect::<Vec<_>>();

        for source in 0..vertex_count {
            let row = source * row_words;
            for &initial_state in &initial {
                for target in
                    closure.reachable_indices_unordered(source * state_count + initial_state)
                {
                    if !automaton.is_accepting((target % state_count) as StateId) {
                        continue;
                    }
                    let target = target / state_count;
                    if !insert_bit(&mut forward[row..row + row_words], target) {
                        continue;
                    }
                    insert_bit(
                        &mut reverse[target * row_words..(target + 1) * row_words],
                        source,
                    );
                    pair_count += 1;
                    insert_bit(&mut starts, source);
                    insert_bit(&mut ends, target);
                    if source == target {
                        insert_bit(&mut diagonal, source);
                    }
                }
            }
        }

        Self {
            row_words,
            forward,
            reverse,
            starts,
            ends,
            diagonal,
            pair_count,
        }
    }

    fn row(&self, source: usize) -> &[u64] {
        let start = source * self.row_words;
        &self.forward[start..start + self.row_words]
    }

    fn reverse_row(&self, target: usize) -> &[u64] {
        let start = target * self.row_words;
        &self.reverse[start..start + self.row_words]
    }

    fn contains(&self, source: usize, target: usize) -> bool {
        self.row(source)[target / u64::BITS as usize] & (1u64 << (target % u64::BITS as usize)) != 0
    }

    fn row_indices(&self, source: usize) -> BitIndexes<'_> {
        BitIndexes::new(self.row(source))
    }
}

struct BitIndexes<'a> {
    words: &'a [u64],
    next_word: usize,
    current_word_index: usize,
    current_word: u64,
}

impl<'a> BitIndexes<'a> {
    fn new(words: &'a [u64]) -> Self {
        Self {
            words,
            next_word: 0,
            current_word_index: 0,
            current_word: 0,
        }
    }
}

impl Iterator for BitIndexes<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros() as usize;
                self.current_word &= self.current_word - 1;
                return Some(self.current_word_index * u64::BITS as usize + bit);
            }
            self.current_word = *self.words.get(self.next_word)?;
            self.current_word_index = self.next_word;
            self.next_word += 1;
        }
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
        build_stats.accepted_bitset_words = accepted.forward.len();
        build_stats.accepted_accelerator_words = accepted
            .reverse
            .len()
            .saturating_add(accepted.starts.len())
            .saturating_add(accepted.ends.len())
            .saturating_add(accepted.diagonal.len());
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
                    .row_indices(source_index)
                    .map(move |target_index| (source, self.vertices[target_index]))
            })
    }

    /// Sorted, duplicate-free accepted targets for one source.
    pub fn reachable_from<'a>(
        &'a self,
        source: &RawInline,
    ) -> impl Iterator<Item = RawInline> + 'a {
        self.values(self.forward_bits(source))
    }

    /// Sorted, duplicate-free accepted sources for one target.
    pub fn reaching<'a>(&'a self, target: &RawInline) -> impl Iterator<Item = RawInline> + 'a {
        self.values(self.reverse_bits(target))
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
            accepted_pairs: self.accepted.pair_count,
        }
    }

    pub(crate) fn forward_bits(&self, source: &RawInline) -> &[u64] {
        self.vertices
            .binary_search(source)
            .ok()
            .map(|source| self.accepted.row(source))
            .unwrap_or(&[])
    }

    pub(crate) fn reverse_bits(&self, target: &RawInline) -> &[u64] {
        self.vertices
            .binary_search(target)
            .ok()
            .map(|target| self.accepted.reverse_row(target))
            .unwrap_or(&[])
    }

    pub(crate) fn starts_bits(&self) -> &[u64] {
        &self.accepted.starts
    }

    pub(crate) fn ends_bits(&self) -> &[u64] {
        &self.accepted.ends
    }

    pub(crate) fn diagonal_bits(&self) -> &[u64] {
        &self.accepted.diagonal
    }

    pub(crate) fn values<'a>(&'a self, bits: &'a [u64]) -> impl Iterator<Item = RawInline> + 'a {
        BitIndexes::new(bits).map(|vertex| self.vertices[vertex])
    }

    pub(crate) fn bits_contain(&self, bits: &[u64], value: &RawInline) -> bool {
        self.vertices
            .binary_search(value)
            .is_ok_and(|vertex| bit_is_set(bits, vertex))
    }
}

fn insert_bit(words: &mut [u64], bit: usize) -> bool {
    let mask = 1u64 << (bit % u64::BITS as usize);
    let word = &mut words[bit / u64::BITS as usize];
    let inserted = *word & mask == 0;
    *word |= mask;
    inserted
}

pub(crate) fn bit_is_set(words: &[u64], bit: usize) -> bool {
    words
        .get(bit / u64::BITS as usize)
        .is_some_and(|word| word & (1u64 << (bit % u64::BITS as usize)) != 0)
}

pub(crate) fn bit_count(words: &[u64]) -> usize {
    words.iter().map(|word| word.count_ones() as usize).sum()
}
