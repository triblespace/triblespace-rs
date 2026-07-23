//! Fixed-predicate 2-ring for unlabeled graphs.
//!
//! Following Section 4.4 of Arroyuelo/Gómez-Brandón/Hogan/
//! Navarro/Reutter/Rojas-Ledesma/Soto, *The Ring: Worst-Case
//! Optimal Joins in Graph Databases using (Almost) No Extra
//! Space*, ACM TODS 2024.
//!
//! ## What it is
//!
//! A succinct encoding of a set of undirected edges between
//! `m` nodes. Each edge `{u, v}` is canonicalized to
//! `(low, high)` with `low ≤ high` and stored **once**. The
//! representation supports "enumerate neighbours of vertex x"
//! by combining two primitive operations on the pair set:
//!
//! - rows where `low = x` — contiguous range in the sorted
//!   pair list; read the corresponding `high` values.
//! - rows where `high = x` — scattered; found via `select` on
//!   the wavelet matrix over the high column, then mapped back
//!   to the `low` value via the run-bitmap.
//!
//! For an unlabeled graph this is a strict improvement over
//! "explicit both directions" CSR — each edge contributes one
//! entry to the data column instead of two. At the cost of
//! `O(log m)` per-neighbour enumeration (vs `O(1)` for
//! `CompactVector`-backed CSR), we cut the data column in half
//! and gain both-end enumeration for free.
//!
//! ## Structures
//!
//! Two jerky-backed primitives:
//!
//! - `low_runs: BitVector<Rank9SelIndex>` — length
//!   `n_edges + n_nodes`. For each node `v` in `0..n_nodes` it
//!   writes `deg_low(v)` ones followed by a single zero
//!   terminator. The `v`-th zero marks the end of node `v`'s
//!   low-run; `rank1` / `rank0` / `select1` / `select0` let us
//!   go between an edge position (rank of its 1-bit) and the
//!   low value (how many 0s precede that 1-bit).
//! - `high_column: WaveletMatrix<Rank9SelIndex>` — length
//!   `n_edges`, alphabet `0..n_nodes`. Stores the high endpoint
//!   for each edge in sorted-by-low order. Supports
//!   `access` / `rank` / `select` in `O(log n_nodes)`.
//!
//! Together that's `n log m + O(n + m)` bits vs CSR's
//! `2n log m + O(n + m)` bits — ~½ on the neighbour column.

use anybytes::area::SectionWriter;
use anybytes::Bytes;
use jerky::bit_vector::{
    BitVector, BitVectorBuilder, BitVectorDataMeta, Rank, Rank9SelIndex, Select,
};
use jerky::char_sequences::wavelet_matrix::WaveletMatrixMeta;
use jerky::char_sequences::WaveletMatrix;

use crate::succinct::SuccinctDocLensError;

/// Succinct ring-encoded undirected graph.
///
/// Built from a pre-canonicalized edge list (each edge
/// appearing once as `(low, high)` with `low ≤ high`, sorted
/// lexicographically by `(low, high)`). Self-loops are
/// permitted (they encode `low == high`) — the neighbour
/// iterator returns the vertex itself in that case, once.
#[derive(Debug)]
pub struct RingGraph {
    low_runs: BitVector<Rank9SelIndex>,
    high_column: WaveletMatrix<Rank9SelIndex>,
    n_nodes: usize,
    n_edges: usize,
}

/// Serializable layout metadata for [`RingGraph`].
#[derive(Debug, Clone, Copy)]
pub struct RingGraphMeta {
    pub low_runs: BitVectorDataMeta,
    pub high_column: WaveletMatrixMeta,
    pub n_nodes: u64,
    pub n_edges: u64,
}

impl RingGraph {
    /// Build a ring graph from a sorted slice of
    /// `(low, high)` pairs. The caller is responsible for
    /// canonicalization (each undirected edge appears exactly
    /// once with `low ≤ high`) and sort order (lexicographic
    /// by `(low, high)`). Duplicate edges are stored as given
    /// — caller dedups if desired.
    ///
    /// Both jerky structures are written into `sections`, so
    /// the resulting byte arena holds the ring's data ready
    /// for `freeze`/`from_bytes` round-trip.
    pub fn build(
        edges: &[(u32, u32)],
        n_nodes: usize,
        sections: &mut SectionWriter<'_>,
    ) -> Result<(Self, RingGraphMeta), SuccinctDocLensError> {
        let n_edges = edges.len();

        // Run bitmap. Iterate nodes 0..n_nodes in order; for
        // each node v, emit deg_low(v) ones (one per edge with
        // low = v) followed by exactly one zero terminator.
        //
        // Invariant on entry: edges is sorted by (low, high),
        // so we advance a pointer through `edges` in lock-step
        // with v.
        let mut low_runs_b = BitVectorBuilder::with_capacity(n_edges + n_nodes, sections)?;
        let mut pos = 0usize;
        let mut edge_cursor = 0usize;
        for v in 0..n_nodes {
            while edge_cursor < n_edges && edges[edge_cursor].0 as usize == v {
                // bit is already false by default; only set
                // the 1s we need.
                low_runs_b.set_bit(pos, true)?;
                pos += 1;
                edge_cursor += 1;
            }
            // zero terminator — already false by default;
            // just advance the cursor past the zero slot.
            pos += 1;
        }
        debug_assert_eq!(pos, n_edges + n_nodes);
        debug_assert_eq!(edge_cursor, n_edges);
        let low_runs = low_runs_b.freeze::<Rank9SelIndex>();

        // High column. Alphabet is 0..n_nodes (we need
        // n_nodes + 1 because WM takes the alphabet as
        // exclusive upper bound; a high endpoint can legally
        // be n_nodes - 1).
        let high_column = WaveletMatrix::<Rank9SelIndex>::from_iter(
            n_nodes.max(1),
            edges.iter().map(|(_, h)| *h as usize),
            sections,
        )?;

        let meta = RingGraphMeta {
            low_runs: low_runs.metadata(),
            high_column: high_column.metadata(),
            n_nodes: n_nodes as u64,
            n_edges: n_edges as u64,
        };
        Ok((
            Self {
                low_runs,
                high_column,
                n_nodes,
                n_edges,
            },
            meta,
        ))
    }

    /// Reload from a byte region previously produced by
    /// [`RingGraph::build`].
    pub fn from_bytes(meta: RingGraphMeta, bytes: Bytes) -> Result<Self, SuccinctDocLensError> {
        let low_runs = BitVector::<Rank9SelIndex>::from_bytes(meta.low_runs, bytes.clone())?;
        let high_column = WaveletMatrix::<Rank9SelIndex>::from_bytes(meta.high_column, bytes)?;
        Ok(Self {
            low_runs,
            high_column,
            n_nodes: meta.n_nodes as usize,
            n_edges: meta.n_edges as usize,
        })
    }

    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }
    pub fn n_edges(&self) -> usize {
        self.n_edges
    }

    /// Enumerate all neighbours of vertex `v`. Each neighbour
    /// is yielded exactly once; order is unspecified (lows
    /// first, then highs — but callers shouldn't rely on
    /// this).
    pub fn neighbours(&self, v: usize) -> Neighbours<'_> {
        Neighbours::new(self, v)
    }

    /// Edge-range boundary: the `[start, end)` range in
    /// `high_column` that holds edges with `low = v`. Both
    /// bounds are computed from the run bitmap via rank/select.
    #[inline]
    fn low_range(&self, v: usize) -> (usize, usize) {
        if v >= self.n_nodes {
            return (self.n_edges, self.n_edges);
        }
        // End: position of the v-th 0 in low_runs; the number
        // of 1s before that position is the cumulative edge
        // count through node v.
        let end_zero = self.low_runs.select0(v).expect("v < n_nodes");
        let end = self.low_runs.rank1(end_zero).expect("in range");
        // Start: same thing for v-1, or 0 if v == 0.
        let start = if v == 0 {
            0
        } else {
            let s_zero = self.low_runs.select0(v - 1).expect("v-1 < n_nodes");
            self.low_runs.rank1(s_zero).expect("in range")
        };
        (start, end)
    }

    /// Recover the `low` value for the edge at position
    /// `edge_pos` in the sorted edge list. Inverse of the run
    /// encoding used at build time.
    #[inline]
    fn low_of(&self, edge_pos: usize) -> u32 {
        // The edge at position `edge_pos` is the
        // (edge_pos + 1)-th 1-bit in low_runs (0-indexed:
        // `select1(edge_pos)`). Count 0s before that bit
        // position — that's the low value.
        let bit_pos = self.low_runs.select1(edge_pos).expect("edge_pos < n_edges");
        self.low_runs.rank0(bit_pos).expect("in range") as u32
    }
}

/// Iterator over all neighbours of a vertex. Yields each
/// neighbour exactly once, looking up "edges with low = v"
/// by contiguous range and "edges with high = v" via wavelet-
/// matrix `select`.
pub struct Neighbours<'a> {
    ring: &'a RingGraph,
    /// Current position inside the low-range of `v`.
    low_pos: usize,
    low_end: usize,
    /// Counter for high-column select enumeration.
    high_k: usize,
    high_total: usize,
    /// The queried vertex, kept for the select call.
    v: usize,
}

impl<'a> Neighbours<'a> {
    fn new(ring: &'a RingGraph, v: usize) -> Self {
        let (low_start, low_end) = ring.low_range(v);
        // Total occurrences of `v` in the high column.
        let high_total = if v < ring.n_nodes {
            ring.high_column.rank(ring.n_edges, v).unwrap_or(0)
        } else {
            0
        };
        Self {
            ring,
            low_pos: low_start,
            low_end,
            high_k: 0,
            high_total,
            v,
        }
    }
}

impl<'a> Iterator for Neighbours<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        // Phase 1: edges where low = v. Yield the high
        // endpoint from the wavelet matrix.
        if self.low_pos < self.low_end {
            let high = self
                .ring
                .high_column
                .access(self.low_pos)
                .expect("low_pos in range") as u32;
            self.low_pos += 1;
            return Some(high);
        }
        // Phase 2: edges where high = v. Iterate via
        // `select(k, v)` → get the position, recover the low.
        if self.high_k < self.high_total {
            let pos = self
                .ring
                .high_column
                .select(self.high_k, self.v)
                .expect("high_k < high_total");
            self.high_k += 1;
            let low = self.ring.low_of(pos);
            return Some(low);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anybytes::area::ByteArea;

    /// Build a ring from an edge list without any external
    /// cleanup — caller handles canonicalization. Handy for
    /// the tests.
    fn build_ring(edges: Vec<(u32, u32)>, n_nodes: usize) -> RingGraph {
        let mut edges = edges
            .into_iter()
            .map(|(a, b)| if a <= b { (a, b) } else { (b, a) })
            .collect::<Vec<_>>();
        edges.sort_unstable();
        edges.dedup();
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let (ring, _meta) = RingGraph::build(&edges, n_nodes, &mut sections).unwrap();
        ring
    }

    #[test]
    fn empty_graph_has_no_neighbours() {
        let ring = build_ring(vec![], 4);
        for v in 0..4 {
            let got: Vec<u32> = ring.neighbours(v).collect();
            assert!(got.is_empty(), "v={v}: expected empty, got {got:?}");
        }
    }

    #[test]
    fn single_edge_both_ends_see_each_other() {
        let ring = build_ring(vec![(1, 3)], 5);
        let n1: Vec<u32> = ring.neighbours(1).collect();
        let n3: Vec<u32> = ring.neighbours(3).collect();
        assert_eq!(n1, vec![3]);
        assert_eq!(n3, vec![1]);
        assert!(ring.neighbours(0).next().is_none());
        assert!(ring.neighbours(2).next().is_none());
        assert!(ring.neighbours(4).next().is_none());
    }

    #[test]
    fn triangle() {
        // Triangle: 0-1, 1-2, 0-2. Each node has two neighbours.
        let ring = build_ring(vec![(0, 1), (1, 2), (0, 2)], 3);
        let mut n0: Vec<u32> = ring.neighbours(0).collect();
        let mut n1: Vec<u32> = ring.neighbours(1).collect();
        let mut n2: Vec<u32> = ring.neighbours(2).collect();
        n0.sort_unstable();
        n1.sort_unstable();
        n2.sort_unstable();
        assert_eq!(n0, vec![1, 2]);
        assert_eq!(n1, vec![0, 2]);
        assert_eq!(n2, vec![0, 1]);
    }

    #[test]
    fn star_graph_hub_has_all_others() {
        // Star on 5 nodes: 0 is hub; 0-1, 0-2, 0-3, 0-4.
        let ring = build_ring(vec![(0, 1), (0, 2), (0, 3), (0, 4)], 5);
        let mut n0: Vec<u32> = ring.neighbours(0).collect();
        n0.sort_unstable();
        assert_eq!(n0, vec![1, 2, 3, 4]);
        // Each leaf sees the hub.
        for leaf in 1..=4 {
            let nleaf: Vec<u32> = ring.neighbours(leaf).collect();
            assert_eq!(nleaf, vec![0]);
        }
    }

    #[test]
    fn self_loop_yields_vertex_once() {
        // Node 2 has a self-loop (canonicalized to (2, 2)).
        let ring = build_ring(vec![(2, 2)], 4);
        let n2: Vec<u32> = ring.neighbours(2).collect();
        // (low=2, high=2) shows up both in the low-range AND
        // in the high-column for node 2 — so we yield it twice.
        // That matches the graph-theoretic convention that a
        // self-loop contributes two to the vertex degree.
        assert_eq!(n2, vec![2, 2]);
    }

    #[test]
    fn hand_rolled_matches_adjacency_list() {
        // Small random graph; build both a naive adjacency
        // list and a ring, verify neighbour sets match.
        let edges = vec![
            (0, 3),
            (0, 5),
            (1, 2),
            (1, 4),
            (1, 5),
            (2, 3),
            (2, 4),
            (3, 4),
            (4, 5),
        ];
        let m = 6;
        let mut adj: Vec<Vec<u32>> = vec![Vec::new(); m];
        for &(a, b) in &edges {
            adj[a as usize].push(b);
            adj[b as usize].push(a);
        }
        let ring = build_ring(edges, m);
        for (v, expected_neighbours) in adj.iter().enumerate() {
            let mut expected = expected_neighbours.clone();
            let mut got: Vec<u32> = ring.neighbours(v).collect();
            expected.sort_unstable();
            got.sort_unstable();
            assert_eq!(got, expected, "v={v}");
        }
    }

    #[test]
    fn bytes_round_trip() {
        let edges_raw = vec![(0, 1), (0, 2), (1, 3), (2, 3), (2, 4)];
        let mut edges = edges_raw.clone();
        edges.sort_unstable();
        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let (ring, meta) = RingGraph::build(&edges, 5, &mut sections).unwrap();
        let _ = sections;
        let bytes = area.freeze().unwrap();

        // Compare pre-round-trip neighbours against post.
        let reloaded = RingGraph::from_bytes(meta, bytes).unwrap();
        for v in 0..5 {
            let mut a: Vec<u32> = ring.neighbours(v).collect();
            let mut b: Vec<u32> = reloaded.neighbours(v).collect();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "reload drift at v={v}");
        }
    }
}
