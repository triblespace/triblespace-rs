//! End-to-end HNSW query latency: CSR vs ring, measured
//! against realistic query cost (handle fetch via
//! `BlobCache` + cosine distance).
//!
//! The `ring_vs_csr` example compared raw neighbour-
//! enumeration (~100-165× slower for ring). This one runs
//! the whole greedy-descent + ef-search walk, so we can see
//! whether the graph-layer slowdown matters against the
//! dominating distance-eval cost.
//!
//! ```sh
//! cargo run --release --example ring_vs_csr_hnsw
//! ```
//!
//! Note: the walk logic is duplicated from
//! `SuccinctHNSWIndex::similar` rather than refactored,
//! because this is a benchmark; refactoring the production
//! types to parameterize over the graph backend can come
//! next if the ring wins the trade.

use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;

use anybytes::area::ByteArea;

use triblespace_core::blob::{BlobCache, MemoryBlobStore};
use triblespace_core::repo::BlobStore;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Inline;

use triblespace_search::hnsw::HNSWBuilder;
use triblespace_search::ring::RingGraph;
use triblespace_search::schemas::{put_embedding, Embedding};
use triblespace_search::succinct::SuccinctGraph;

/// SplitMix64 — deterministic, no extra deps.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn f32_pm1(&mut self) -> f32 {
        (self.next() as i32 as f32) / (i32::MAX as f32)
    }
}

/// Build an HNSW index at the given scale. Returns the
/// naive index (we read the graph structure out of it for
/// both encodings) and the underlying `MemoryBlobStore` with
/// the embedding blobs already put.
fn build_hnsw(
    n: usize,
    dim: usize,
    seed: u64,
) -> (
    triblespace_search::testing::HNSWIndex,
    MemoryBlobStore,
) {
    let mut rng = Rng(seed);
    let mut store = MemoryBlobStore::new();
    let mut b = HNSWBuilder::new(dim).with_seed(seed);
    for _ in 0..n {
        let vec: Vec<f32> = (0..dim).map(|_| rng.f32_pm1()).collect();
        let h = put_embedding::<_>(&mut store, vec.clone()).unwrap();
        b.insert(h, vec).unwrap();
    }
    (b.build_naive(), store)
}

/// Encode the HNSW graph as the existing `SuccinctGraph`
/// (CSR per (layer, node) with shared padding).
fn encode_csr(
    naive: &triblespace_search::testing::HNSWIndex,
) -> (usize, SuccinctGraph) {
    let n = naive.doc_count();
    let n_layers = naive.max_level() as usize + 1;
    let mut layers: Vec<Vec<Vec<u32>>> =
        (0..n_layers).map(|_| vec![Vec::new(); n]).collect();
    for i in 0..n {
        let lvl = naive.node_level(i).unwrap() as usize;
        for l in 0..=lvl {
            layers[l][i] = naive.node_neighbours(i, l as u8).to_vec();
        }
    }
    let (bytes, meta) = SuccinctGraph::build(&layers, n).expect("build CSR");
    let size = bytes.len();
    let graph = SuccinctGraph::from_bytes(meta, bytes).expect("reload CSR");
    (size, graph)
}

/// Encode the HNSW graph as one `RingGraph` per layer.
/// Each layer's edge list is canonicalized to undirected
/// `(low, high)` pairs, deduplicated, sorted.
fn encode_ring(
    naive: &triblespace_search::testing::HNSWIndex,
) -> (usize, Vec<RingGraph>) {
    let n = naive.doc_count();
    let n_layers = naive.max_level() as usize + 1;

    // One ByteArea per layer — each ring gets its own byte
    // region. Sum the sizes for the headline number.
    let mut rings: Vec<RingGraph> = Vec::with_capacity(n_layers);
    let mut total_bytes = 0usize;
    for l in 0..n_layers {
        let mut edges: Vec<(u32, u32)> = Vec::new();
        for i in 0..n {
            if (naive.node_level(i).unwrap() as usize) < l {
                continue;
            }
            for &nb in naive.node_neighbours(i, l as u8) {
                let (lo, hi) = if (i as u32) < nb {
                    (i as u32, nb)
                } else {
                    (nb, i as u32)
                };
                edges.push((lo, hi));
            }
        }
        edges.sort_unstable();
        edges.dedup();

        let mut area = ByteArea::new().unwrap();
        let mut sections = area.sections();
        let (ring, _meta) = RingGraph::build(&edges, n, &mut sections).unwrap();
        let _ = sections;
        let bytes = area.freeze().unwrap();
        total_bytes += bytes.len();
        rings.push(ring);
    }
    (total_bytes, rings)
}

// ─ Heap wrappers for ef-search (min-heap on dist, max-heap on dist). ─
#[derive(Clone, Copy)]
struct MinDist {
    idx: u32,
    dist: f32,
}
impl PartialEq for MinDist {
    fn eq(&self, o: &Self) -> bool {
        self.dist == o.dist
    }
}
impl Eq for MinDist {}
impl PartialOrd for MinDist {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for MinDist {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.dist.partial_cmp(&self.dist).unwrap_or(std::cmp::Ordering::Equal)
    }
}
#[derive(Clone, Copy)]
struct MaxDist {
    idx: u32,
    dist: f32,
}
impl PartialEq for MaxDist {
    fn eq(&self, o: &Self) -> bool {
        self.dist == o.dist
    }
}
impl Eq for MaxDist {}
impl PartialOrd for MaxDist {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for MaxDist {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.dist.partial_cmp(&o.dist).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// The shared HNSW walk, parameterised over how to list a
/// node's neighbours on a given layer and over the distance
/// function. Distance in this benchmark is always a BlobCache
/// fetch + cosine — that's the production path and the
/// suspected bottleneck. The closure shape keeps us from
/// having to name the concrete BlobCache type.
fn walk<F, D>(
    entry: u32,
    max_level: u8,
    query: &[f32],
    k: usize,
    ef: usize,
    mut neighbours_on_layer: F,
    mut dist_fn: D,
) -> Vec<(u32, f32)>
where
    F: FnMut(u32, u8) -> Vec<u32>,
    D: FnMut(u32) -> f32,
{
    let mut dist = |i: u32| dist_fn(i);
    let _ = query; // query is captured by `dist_fn`; keep the arg so the
                   // caller side reads naturally, even though `walk` never
                   // uses it directly now that the dist closure owns it.

    // Greedy descent.
    let mut curr = entry;
    for lvl in (1..=max_level).rev() {
        loop {
            let mut changed = false;
            let neigh = neighbours_on_layer(curr, lvl);
            let mut curr_d = dist(curr);
            for n in neigh {
                let d = dist(n);
                if d < curr_d {
                    curr_d = d;
                    curr = n;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    // ef-search on layer 0.
    let mut visited: HashSet<u32> = HashSet::new();
    visited.insert(curr);
    let d0 = dist(curr);
    let mut cands: BinaryHeap<MinDist> = BinaryHeap::new();
    cands.push(MinDist { idx: curr, dist: d0 });
    let mut results: BinaryHeap<MaxDist> = BinaryHeap::new();
    results.push(MaxDist { idx: curr, dist: d0 });
    while let Some(c) = cands.pop() {
        let farthest = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
        if c.dist > farthest && results.len() >= ef {
            break;
        }
        for n in neighbours_on_layer(c.idx, 0) {
            if !visited.insert(n) {
                continue;
            }
            let d = dist(n);
            let farthest = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
            if d < farthest || results.len() < ef {
                cands.push(MinDist { idx: n, dist: d });
                results.push(MaxDist { idx: n, dist: d });
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }
    let mut ranked: Vec<(u32, f32)> =
        results.into_iter().map(|m| (m.idx, 1.0 - m.dist)).collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    ranked.truncate(k);
    ranked
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
fn fmt_ns(n: u128) -> String {
    if n >= 1_000_000 {
        format!("{:.2} ms", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2} µs", n as f64 / 1_000.0)
    } else {
        format!("{n} ns")
    }
}
fn fmt_bytes(n: usize) -> String {
    if n >= 1 << 20 {
        format!("{:.1} MiB", n as f64 / (1 << 20) as f64)
    } else if n >= 1 << 10 {
        format!("{:.1} KiB", n as f64 / (1 << 10) as f64)
    } else {
        format!("{n} B")
    }
}

fn bench(n: usize, dim: usize, k: usize, ef: usize, seed: u64) {
    println!("\n── n = {n}   dim = {dim}   k = {k}   ef = {ef} ──");

    let (naive, mut store) = build_hnsw(n, dim, seed);
    let handles: Vec<Inline<Handle<Embedding>>> = naive.handles().to_vec();
    let entry = naive.entry_point().expect("non-empty");
    let max_level = naive.max_level();
    // Sanity: a realistic HNSW index at 10k-100k sits on
    // ~4-7 levels with the default M.
    println!("   built HNSW: max_level = {}", max_level);

    let (csr_size, csr) = encode_csr(&naive);
    let (ring_size, rings) = encode_ring(&naive);
    println!(
        "   CSR  graph blob: {}",
        fmt_bytes(csr_size),
    );
    println!(
        "   Ring graph blob: {}   ({:.2}× CSR)",
        fmt_bytes(ring_size),
        ring_size as f64 / csr_size as f64,
    );

    let reader = store.reader().unwrap();

    let mut rng = Rng(seed ^ 0xFACE_FEED);
    let queries: Vec<Vec<f32>> = (0..100)
        .map(|_| (0..dim).map(|_| rng.f32_pm1()).collect())
        .collect();

    // ─ CSR run ─
    let cache_csr = BlobCache::new(reader.clone());
    let neigh_csr = |v: u32, l: u8| -> Vec<u32> {
        csr.neighbours(v as usize, l as usize).collect()
    };
    // Warm the cache (one query) before timing.
    {
        let q = &queries[0];
        let dist = |i: u32| {
            let view: std::sync::Arc<anybytes::View<[f32]>> =
                cache_csr.get(handles[i as usize]).unwrap();
            let v: &[f32] = view.as_ref().as_ref();
            1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
        };
        let _ = walk(entry, max_level, q, k, ef, neigh_csr, dist);
    }
    let mut samples_csr: Vec<u128> = Vec::with_capacity(queries.len());
    for q in &queries {
        let dist = |i: u32| {
            let view: std::sync::Arc<anybytes::View<[f32]>> =
                cache_csr.get(handles[i as usize]).unwrap();
            let v: &[f32] = view.as_ref().as_ref();
            1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
        };
        let t = Instant::now();
        let _ = walk(entry, max_level, q, k, ef, neigh_csr, dist);
        samples_csr.push(t.elapsed().as_nanos());
    }
    samples_csr.sort_unstable();
    let csr_avg = samples_csr.iter().sum::<u128>() / samples_csr.len() as u128;

    // ─ Ring run ─
    let cache_ring = BlobCache::new(reader.clone());
    let neigh_ring = |v: u32, l: u8| -> Vec<u32> {
        rings[l as usize].neighbours(v as usize).collect()
    };
    {
        let q = &queries[0];
        let dist = |i: u32| {
            let view: std::sync::Arc<anybytes::View<[f32]>> =
                cache_ring.get(handles[i as usize]).unwrap();
            let v: &[f32] = view.as_ref().as_ref();
            1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
        };
        let _ = walk(entry, max_level, q, k, ef, neigh_ring, dist);
    }
    let mut samples_ring: Vec<u128> = Vec::with_capacity(queries.len());
    for q in &queries {
        let dist = |i: u32| {
            let view: std::sync::Arc<anybytes::View<[f32]>> =
                cache_ring.get(handles[i as usize]).unwrap();
            let v: &[f32] = view.as_ref().as_ref();
            1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
        };
        let t = Instant::now();
        let _ = walk(entry, max_level, q, k, ef, neigh_ring, dist);
        samples_ring.push(t.elapsed().as_nanos());
    }
    samples_ring.sort_unstable();
    let ring_avg = samples_ring.iter().sum::<u128>() / samples_ring.len() as u128;

    println!(
        "   CSR  walk:  avg {:<10}  p50 {:<10}  p99 {:<10}",
        fmt_ns(csr_avg),
        fmt_ns(percentile(&samples_csr, 0.5)),
        fmt_ns(percentile(&samples_csr, 0.99)),
    );
    println!(
        "   Ring walk:  avg {:<10}  p50 {:<10}  p99 {:<10}   ({:.2}× CSR)",
        fmt_ns(ring_avg),
        fmt_ns(percentile(&samples_ring, 0.5)),
        fmt_ns(percentile(&samples_ring, 0.99)),
        ring_avg as f64 / csr_avg.max(1) as f64,
    );

    // Top-k recall: fraction of CSR's top-k that ring also
    // returned. Identical graph topology + deterministic
    // PRNG means ties resolve identically (up to heap
    // ordering), so recall should be ~1.0.
    let q = &queries[0];
    let dist_csr = |i: u32| {
        let view: std::sync::Arc<anybytes::View<[f32]>> =
            cache_csr.get(handles[i as usize]).unwrap();
        let v: &[f32] = view.as_ref().as_ref();
        1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
    };
    let dist_ring = |i: u32| {
        let view: std::sync::Arc<anybytes::View<[f32]>> =
            cache_ring.get(handles[i as usize]).unwrap();
        let v: &[f32] = view.as_ref().as_ref();
        1.0 - q.iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f32>()
    };
    let csr_top: HashSet<u32> =
        walk(entry, max_level, q, k, ef, neigh_csr, dist_csr)
            .into_iter()
            .map(|(i, _)| i)
            .collect();
    let ring_top: HashSet<u32> =
        walk(entry, max_level, q, k, ef, neigh_ring, dist_ring)
            .into_iter()
            .map(|(i, _)| i)
            .collect();
    let overlap = csr_top.intersection(&ring_top).count();
    println!(
        "   top-{k} overlap on sample query: {overlap}/{}",
        csr_top.len()
    );
}

fn main() {
    println!("End-to-end HNSW query latency: CSR vs ring backend.");
    println!("Distance eval goes through BlobCache<MemoryBlobStore, Embedding>");
    println!("— the same code path SH25::similar would use in production.");

    bench(1_000, 32, 10, 50, 0x1234);
    bench(10_000, 32, 10, 50, 0x5678);
    bench(10_000, 128, 10, 50, 0xDEAD);
    bench(50_000, 128, 10, 50, 0xBEEF);
    // Keep the ignore-but-available; 100k gets slow on build
    // in the example but you can uncomment if you want.
    // bench(100_000, 128, 10, 50, 0xFACE);
}
