//! Small deterministic measurement harness for the exact path index.
//!
//! Run with:
//!
//! ```text
//! cargo run -p triblespace-paths --release --example measure
//! ```
//!
//! The graph construction and segment assignment are deterministic. Timings
//! are reported as min/median/max over repeated runs and will naturally vary
//! between machines. Leaf distributions contain one sorted value per segment.

use std::time::Instant;

use triblespace_core::id::RawId;
use triblespace_core::inline::RawInline;
use triblespace_paths::{
    Automaton, BuildStats, GraphEdge, IndexMetrics, PathIndex, Step, Transition,
};

const ATTRIBUTE: RawId = [0x5a; 16];
const REPETITIONS: usize = 5;
const SEGMENTS: usize = 4;
const SCALES: [usize; 3] = [16, 32, 64];

#[derive(Clone, Copy)]
enum GraphFamily {
    Chain,
    Ring,
    BowTie,
}

impl GraphFamily {
    const ALL: [Self; 3] = [Self::Chain, Self::Ring, Self::BowTie];

    fn name(self) -> &'static str {
        match self {
            Self::Chain => "chain",
            Self::Ring => "ring",
            Self::BowTie => "bow_tie",
        }
    }

    fn edges(self, scale: usize) -> Vec<GraphEdge> {
        match self {
            Self::Chain => (0..scale - 1).map(|at| edge(at, at + 1)).collect(),
            Self::Ring => (0..scale).map(|at| edge(at, (at + 1) % scale)).collect(),
            Self::BowTie => {
                let hub = scale / 2;
                (0..hub)
                    .map(|source| edge(source, hub))
                    .chain((hub + 1..scale).map(|target| edge(hub, target)))
                    .collect()
            }
        }
    }
}

#[derive(Debug)]
struct TimingSummary {
    min_ns: u128,
    median_ns: u128,
    max_ns: u128,
}

#[derive(Debug)]
struct Measurement {
    leaf_build: TimingSummary,
    merge: TimingSummary,
    monolithic_build: TimingSummary,
    batch_sizes: Vec<usize>,
    leaf_stats: Vec<BuildStats>,
    merge_stats: BuildStats,
    monolithic_stats: BuildStats,
    metrics: IndexMetrics,
}

fn main() {
    println!("triblespace-paths deterministic measurement harness");
    println!(
        "configuration repetitions={REPETITIONS} warmups=1 segments={SEGMENTS} scales={SCALES:?}"
    );
    println!("leaf distributions are sorted values with one observation per segment");
    println!("rectangle_log2 histograms use k:count for areas in 2^k..2^(k+1)");

    for family in GraphFamily::ALL {
        for scale in SCALES {
            let edges = family.edges(scale);
            let measurement = measure(&edges);
            report(family, scale, edges.len(), &measurement);
        }
    }
}

fn measure(edges: &[GraphEdge]) -> Measurement {
    let batches = partition(edges, SEGMENTS);
    let batch_sizes = batches.iter().map(Vec::len).collect::<Vec<_>>();
    let mut leaf_samples = Vec::with_capacity(REPETITIONS);
    let mut merge_samples = Vec::with_capacity(REPETITIONS);
    let mut monolithic_samples = Vec::with_capacity(REPETITIONS);
    let mut snapshot = None;

    // The first iteration warms allocator and instruction-cache paths but is
    // still checked for exact equivalence with the monolithic construction.
    for iteration in 0..=REPETITIONS {
        let automaton = one_or_more();

        let started = Instant::now();
        let monolithic = PathIndex::from_edges(automaton.clone(), edges.iter().copied());
        let monolithic_ns = started.elapsed().as_nanos();

        let started = Instant::now();
        let leaves = batches
            .iter()
            .map(|batch| PathIndex::from_edges(automaton.clone(), batch.iter().copied()))
            .collect::<Vec<_>>();
        let leaf_ns = started.elapsed().as_nanos();

        let started = Instant::now();
        let merged = PathIndex::merge_all(leaves.iter()).expect("one shared canonical automaton");
        let merge_ns = started.elapsed().as_nanos();

        assert_eq!(merged.metrics(), monolithic.metrics());
        assert!(merged.accepted_pairs().eq(monolithic.accepted_pairs()));
        assert!(merged.product_pairs().eq(monolithic.product_pairs()));

        if snapshot.is_none() {
            snapshot = Some((
                leaves.iter().map(PathIndex::build_stats).collect(),
                merged.build_stats(),
                monolithic.build_stats(),
                merged.metrics(),
            ));
        }

        if iteration != 0 {
            leaf_samples.push(leaf_ns);
            merge_samples.push(merge_ns);
            monolithic_samples.push(monolithic_ns);
        }
    }

    let (leaf_stats, merge_stats, monolithic_stats, metrics) =
        snapshot.expect("the warmup always produces a snapshot");
    Measurement {
        leaf_build: summarize_times(leaf_samples),
        merge: summarize_times(merge_samples),
        monolithic_build: summarize_times(monolithic_samples),
        batch_sizes,
        leaf_stats,
        merge_stats,
        monolithic_stats,
        metrics,
    }
}

fn report(family: GraphFamily, scale: usize, edge_count: usize, result: &Measurement) {
    let metrics = result.metrics;
    let accepted_density = density(
        metrics.accepted_pairs,
        metrics.vertices.saturating_mul(metrics.vertices),
    );

    println!();
    println!(
        "case family={} scale={} edges={} segments={}",
        family.name(),
        scale,
        edge_count,
        result.batch_sizes.len()
    );
    println!(
        "  time_us leaf_build={} merge={} monolithic_build={}",
        format_times(&result.leaf_build),
        format_times(&result.merge),
        format_times(&result.monolithic_build),
    );
    println!(
        "  leaf_distribution batch_edges={} seed_pairs={} effective_insertions={} pairs_added={} derived_pairs={} rectangle_cells={} largest_rectangle={}",
        sorted_values(result.batch_sizes.iter().copied()),
        sorted_values(
            result
                .leaf_stats
                .iter()
                .map(|stats| stats.seed_pairs_considered)
        ),
        sorted_values(
            result
                .leaf_stats
                .iter()
                .map(|stats| stats.effective_insertions)
        ),
        sorted_values(result.leaf_stats.iter().map(|stats| stats.pairs_added)),
        sorted_values(result.leaf_stats.iter().map(|stats| stats.derived_pairs)),
        sorted_values(
            result
                .leaf_stats
                .iter()
                .map(|stats| stats.rectangle_cells_considered)
        ),
        sorted_values(
            result
                .leaf_stats
                .iter()
                .map(|stats| stats.largest_rectangle)
        ),
    );
    println!(
        "  rectangle_log2 leaf_aggregate={} merge={} monolithic={}",
        aggregate_rectangle_histogram(&result.leaf_stats),
        format_rectangle_histogram(&result.merge_stats.rectangle_log2_counts),
        format_rectangle_histogram(&result.monolithic_stats.rectangle_log2_counts),
    );
    println!(
        "  relation vertices={} automaton_states={} product_points={} product_pairs={} product_density={:.6} accepted_pairs={} accepted_density={:.6}",
        metrics.vertices,
        metrics.automaton_states,
        metrics.product_points,
        metrics.product_pairs,
        metrics.product_density(),
        metrics.accepted_pairs,
        accepted_density,
    );
    println!("  merge_work {}", format_stats(result.merge_stats));
    println!(
        "  monolithic_work {}",
        format_stats(result.monolithic_stats)
    );
}

fn partition(edges: &[GraphEdge], requested_segments: usize) -> Vec<Vec<GraphEdge>> {
    let segment_count = requested_segments.min(edges.len()).max(1);
    let mut segments = vec![Vec::new(); segment_count];
    for (index, &edge) in edges.iter().enumerate() {
        // Round-robin assignment deliberately places adjacent path edges in
        // different segments, exercising cross-segment closure during merge.
        segments[index % segment_count].push(edge);
    }
    segments
}

fn one_or_more() -> Automaton {
    Automaton::new(
        2,
        [0],
        [1],
        [
            Transition::new(0, 1, Step::Forward(ATTRIBUTE)),
            Transition::new(1, 1, Step::Forward(ATTRIBUTE)),
        ],
    )
    .expect("the fixed automaton is valid")
}

fn edge(source: usize, target: usize) -> GraphEdge {
    GraphEdge {
        source: vertex(source),
        attribute: ATTRIBUTE,
        target: vertex(target),
    }
}

fn vertex(value: usize) -> RawInline {
    let mut raw = [0; 32];
    raw[..8].copy_from_slice(&(value as u64).to_be_bytes());
    raw
}

fn summarize_times(mut samples: Vec<u128>) -> TimingSummary {
    samples.sort_unstable();
    TimingSummary {
        min_ns: samples[0],
        median_ns: samples[samples.len() / 2],
        max_ns: samples[samples.len() - 1],
    }
}

fn format_times(summary: &TimingSummary) -> String {
    format!(
        "{:.3}/{:.3}/{:.3}",
        summary.min_ns as f64 / 1_000.0,
        summary.median_ns as f64 / 1_000.0,
        summary.max_ns as f64 / 1_000.0,
    )
}

fn sorted_values(values: impl IntoIterator<Item = usize>) -> String {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    format!(
        "[{}]",
        values
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_stats(stats: BuildStats) -> String {
    format!(
        "graph_edges={} seed_pairs={} effective_insertions={} pairs_added={} derived_pairs={} rectangle_cells={} largest_rectangle={}",
        stats.graph_edges,
        stats.seed_pairs_considered,
        stats.effective_insertions,
        stats.pairs_added,
        stats.derived_pairs,
        stats.rectangle_cells_considered,
        stats.largest_rectangle,
    )
}

fn aggregate_rectangle_histogram(stats: &[BuildStats]) -> String {
    let mut counts = vec![0usize; usize::BITS as usize];
    for stats in stats {
        for (total, count) in counts.iter_mut().zip(stats.rectangle_log2_counts) {
            *total = total.saturating_add(count);
        }
    }
    format_rectangle_histogram(&counts)
}

fn format_rectangle_histogram(counts: &[usize]) -> String {
    let buckets = counts
        .iter()
        .enumerate()
        .filter(|(_, count)| **count != 0)
        .map(|(bucket, count)| format!("{bucket}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{buckets}]")
}

fn density(count: usize, possible: usize) -> f64 {
    if possible == 0 {
        0.0
    } else {
        count as f64 / possible as f64
    }
}
