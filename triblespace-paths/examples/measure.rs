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

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use triblespace_core::id::RawId;
use triblespace_core::inline::RawInline;
use triblespace_paths::{
    Automaton, BuildStats, GraphEdge, IndexMetrics, PathIndex, Step, Transition,
};

const ATTRIBUTE: RawId = [0x5a; 16];
const REPETITIONS: usize = 5;
const SEGMENTS: usize = 4;
const SCALES: [usize; 5] = [16, 32, 64, 128, 256];
const BRIDGE_FAN_WIDTHS: [usize; 3] = [32, 64, 128];
const GPU_CROSSOVER_REFERENCE_CELLS: usize = 16_384;
const DEFAULT_EDGE_FILE_LIMIT: usize = 4_096;

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
    leaf_phases: BatchPhases,
    merge_phases: BatchPhases,
    monolithic_phases: BatchPhases,
}

#[derive(Clone, Copy, Debug, Default)]
struct BatchPhases {
    setup_ns: u128,
    scc_ns: u128,
    propagation_ns: u128,
    projection_ns: u128,
}

fn main() {
    let include_large = matches!(std::env::var("PATHS_MEASURE_LARGE"), Ok(value) if value == "1");
    println!("triblespace-paths deterministic measurement harness");
    println!(
        "ablation=scc_condensation_reverse_topological_bitsets (single batch; no rank-one updates)"
    );
    println!(
        "configuration repetitions={REPETITIONS} warmups=1 segments={SEGMENTS} scales={SCALES:?}"
    );
    println!(
        "bridge_fan rectangle_sides={BRIDGE_FAN_WIDTHS:?} side_256={} (enable with PATHS_MEASURE_LARGE=1)",
        if include_large { "enabled" } else { "staged" }
    );
    println!("leaf distributions are sorted values with one observation per segment");
    println!(
        "rectangle_log2 histograms are compatibility accounting, not executed kernel rectangles"
    );
    println!(
        "gpu_crossover_reference={GPU_CROSSOVER_REFERENCE_CELLS} cells (current triblespace-gpu reference, observational only; not path-engine policy)"
    );

    for family in GraphFamily::ALL {
        for scale in SCALES {
            let edges = family.edges(scale);
            let batches = partition(&edges, SEGMENTS);
            let measurement = measure(&edges, &batches, &one_or_more());
            report(family.name(), scale, edges.len(), &measurement);
        }
    }

    for width in BRIDGE_FAN_WIDTHS
        .into_iter()
        .chain(include_large.then_some(256))
    {
        let (edges, batches) = bridge_fan(width);
        let measurement = measure(&edges, &batches, &transitive_closure());
        assert_eq!(measurement.merge_stats.largest_rectangle, width * width);
        assert_eq!(
            measurement.monolithic_stats.largest_rectangle,
            width * width
        );
        report("bridge_fan", width, edges.len(), &measurement);
    }

    if let Ok(paths) = std::env::var("PATHS_MEASURE_EDGE_FILES") {
        let limit = std::env::var("PATHS_MEASURE_EDGE_LIMIT")
            .ok()
            .map(|value| {
                value
                    .parse()
                    .expect("PATHS_MEASURE_EDGE_LIMIT is an integer")
            })
            .unwrap_or(DEFAULT_EDGE_FILE_LIMIT);
        for path in paths.split(',').filter(|path| !path.is_empty()) {
            let edges = load_pipe_edges(Path::new(path), limit);
            let batches = partition(&edges, SEGMENTS);
            let measurement = measure(&edges, &batches, &one_or_more());
            let name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("edge_file");
            report(name, edges.len(), edges.len(), &measurement);
        }
    }
}

fn load_pipe_edges(path: &Path, limit: usize) -> Vec<GraphEdge> {
    let file = File::open(path)
        .unwrap_or_else(|error| panic!("failed to open {}: {error}", path.display()));
    let mut lines = BufReader::new(file).lines();
    lines
        .next()
        .transpose()
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    lines
        .take(limit)
        .enumerate()
        .map(|(index, line)| {
            let line = line.unwrap_or_else(|error| {
                panic!(
                    "failed to read {} line {}: {error}",
                    path.display(),
                    index + 2
                )
            });
            let (source, target) = line.split_once('|').unwrap_or_else(|| {
                panic!(
                    "{} line {} has no pipe delimiter",
                    path.display(),
                    index + 2
                )
            });
            let source = source.parse::<u64>().unwrap_or_else(|error| {
                panic!(
                    "{} line {} has invalid source: {error}",
                    path.display(),
                    index + 2
                )
            });
            let target = target.parse::<u64>().unwrap_or_else(|error| {
                panic!(
                    "{} line {} has invalid target: {error}",
                    path.display(),
                    index + 2
                )
            });
            GraphEdge {
                source: vertex_u64(source),
                attribute: ATTRIBUTE,
                target: vertex_u64(target),
            }
        })
        .collect()
}

fn measure(edges: &[GraphEdge], batches: &[Vec<GraphEdge>], automaton: &Automaton) -> Measurement {
    let batch_sizes = batches.iter().map(Vec::len).collect::<Vec<_>>();
    let mut leaf_samples = Vec::with_capacity(REPETITIONS);
    let mut merge_samples = Vec::with_capacity(REPETITIONS);
    let mut monolithic_samples = Vec::with_capacity(REPETITIONS);
    let mut leaf_phase_samples = Vec::with_capacity(REPETITIONS);
    let mut merge_phase_samples = Vec::with_capacity(REPETITIONS);
    let mut monolithic_phase_samples = Vec::with_capacity(REPETITIONS);
    let mut snapshot = None;

    // The first iteration warms allocator and instruction-cache paths but is
    // still checked for exact equivalence with the monolithic construction.
    for iteration in 0..=REPETITIONS {
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

        let leaf_stats = leaves
            .iter()
            .map(PathIndex::build_stats)
            .collect::<Vec<_>>();
        let merge_stats = merged.build_stats();
        let monolithic_stats = monolithic.build_stats();
        if snapshot.is_none() {
            snapshot = Some((
                leaf_stats.clone(),
                merge_stats,
                monolithic_stats,
                merged.metrics(),
            ));
        }

        if iteration != 0 {
            leaf_samples.push(leaf_ns);
            merge_samples.push(merge_ns);
            monolithic_samples.push(monolithic_ns);
            leaf_phase_samples.push(batch_phases(&leaf_stats));
            merge_phase_samples.push(batch_phases(std::slice::from_ref(&merge_stats)));
            monolithic_phase_samples.push(batch_phases(std::slice::from_ref(&monolithic_stats)));
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
        leaf_phases: summarize_phases(leaf_phase_samples),
        merge_phases: summarize_phases(merge_phase_samples),
        monolithic_phases: summarize_phases(monolithic_phase_samples),
    }
}

fn report(family: &str, scale: usize, edge_count: usize, result: &Measurement) {
    let metrics = result.metrics;
    let accepted_density = density(
        metrics.accepted_pairs,
        metrics.vertices.saturating_mul(metrics.vertices),
    );

    println!();
    println!(
        "case family={} scale={} edges={} segments={}",
        family,
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
        "  batch leaf={} merge={} monolithic={}",
        format_batch_stats(&result.leaf_stats, result.leaf_phases),
        format_batch_stats(
            std::slice::from_ref(&result.merge_stats),
            result.merge_phases,
        ),
        format_batch_stats(
            std::slice::from_ref(&result.monolithic_stats),
            result.monolithic_phases,
        ),
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
        format_rectangle_histogram(
            &result.merge_stats.rectangle_log2_counts,
            &result.merge_stats.rectangle_log2_cells,
        ),
        format_rectangle_histogram(
            &result.monolithic_stats.rectangle_log2_counts,
            &result.monolithic_stats.rectangle_log2_cells,
        ),
    );
    println!(
        "  gpu_reference_ge_{GPU_CROSSOVER_REFERENCE_CELLS} leaf={} merge={} monolithic={}",
        format_crossover(&result.leaf_stats),
        format_crossover(std::slice::from_ref(&result.merge_stats)),
        format_crossover(std::slice::from_ref(&result.monolithic_stats)),
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

fn bridge_fan(width: usize) -> (Vec<GraphEdge>, Vec<Vec<GraphEdge>>) {
    let fan_width = width - 1;
    // The right hub sorts before the left hub so canonical merge seeding
    // installs both fans before the bridge. The bridge therefore presents one
    // `width × width` predecessor × successor rectangle to the closure kernel
    // (each side contains `width - 1` fan vertices plus its hub).
    let right_hub = fan_width;
    let left_hub = fan_width + 1;
    let outer = (0..fan_width)
        .map(|source| edge(source, left_hub))
        .chain((0..fan_width).map(|offset| edge(right_hub, fan_width + 2 + offset)))
        .collect::<Vec<_>>();
    let bridge = vec![edge(left_hub, right_hub)];
    let edges = outer.iter().chain(&bridge).copied().collect::<Vec<_>>();
    (edges, vec![outer, bridge])
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

fn transitive_closure() -> Automaton {
    Automaton::new(
        1,
        [0],
        [0],
        [Transition::new(0, 0, Step::Forward(ATTRIBUTE))],
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
    vertex_u64(value as u64)
}

fn vertex_u64(value: u64) -> RawInline {
    let mut raw = [0; 32];
    raw[..8].copy_from_slice(&value.to_be_bytes());
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

fn format_batch_stats(stats: &[BuildStats], phases: BatchPhases) -> String {
    let sum = |field: fn(&BuildStats) -> usize| stats.iter().map(field).sum::<usize>();
    format!(
        "components={} dag_edges={} bitset_words={} word_ors={} phase_us={:.3}/{:.3}/{:.3}/{:.3}",
        sum(|stats| stats.batch_components),
        sum(|stats| stats.batch_condensation_edges),
        sum(|stats| stats.batch_bitset_words),
        sum(|stats| stats.batch_word_ors),
        phases.setup_ns as f64 / 1_000.0,
        phases.scc_ns as f64 / 1_000.0,
        phases.propagation_ns as f64 / 1_000.0,
        phases.projection_ns as f64 / 1_000.0,
    )
}

fn batch_phases(stats: &[BuildStats]) -> BatchPhases {
    let sum = |field: fn(&BuildStats) -> u128| stats.iter().map(field).sum::<u128>();
    BatchPhases {
        setup_ns: sum(|stats| stats.batch_setup_ns),
        scc_ns: sum(|stats| stats.batch_scc_ns),
        propagation_ns: sum(|stats| stats.batch_propagation_ns),
        projection_ns: sum(|stats| stats.projection_ns),
    }
}

fn summarize_phases(samples: Vec<BatchPhases>) -> BatchPhases {
    BatchPhases {
        setup_ns: median(samples.iter().map(|sample| sample.setup_ns)),
        scc_ns: median(samples.iter().map(|sample| sample.scc_ns)),
        propagation_ns: median(samples.iter().map(|sample| sample.propagation_ns)),
        projection_ns: median(samples.iter().map(|sample| sample.projection_ns)),
    }
}

fn median(values: impl IntoIterator<Item = u128>) -> u128 {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn aggregate_rectangle_histogram(stats: &[BuildStats]) -> String {
    let mut counts = vec![0usize; usize::BITS as usize];
    let mut cells = vec![0usize; usize::BITS as usize];
    for stats in stats {
        for ((total_count, total_cells), (count, bucket_cells)) in
            counts.iter_mut().zip(&mut cells).zip(
                stats
                    .rectangle_log2_counts
                    .into_iter()
                    .zip(stats.rectangle_log2_cells),
            )
        {
            *total_count = total_count.saturating_add(count);
            *total_cells = total_cells.saturating_add(bucket_cells);
        }
    }
    format_rectangle_histogram(&counts, &cells)
}

fn format_rectangle_histogram(counts: &[usize], cells: &[usize]) -> String {
    let buckets = counts
        .iter()
        .zip(cells)
        .enumerate()
        .filter(|(_, (count, _))| **count != 0)
        .map(|(bucket, (count, cells))| format!("{bucket}:{count}/{cells}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{buckets}]")
}

fn format_crossover(stats: &[BuildStats]) -> String {
    let first_bucket = GPU_CROSSOVER_REFERENCE_CELLS.ilog2() as usize;
    let insertions = stats
        .iter()
        .map(|stats| stats.effective_insertions)
        .sum::<usize>();
    let cells = stats
        .iter()
        .map(|stats| stats.rectangle_cells_considered)
        .sum::<usize>();
    let covered_insertions = stats
        .iter()
        .flat_map(|stats| stats.rectangle_log2_counts[first_bucket..].iter())
        .sum::<usize>();
    let covered_cells = stats
        .iter()
        .flat_map(|stats| stats.rectangle_log2_cells[first_bucket..].iter())
        .sum::<usize>();
    format!(
        "insertions={covered_insertions}/{insertions}({:.3}%) cells={covered_cells}/{cells}({:.3}%)",
        percentage(covered_insertions, insertions),
        percentage(covered_cells, cells),
    )
}

fn percentage(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

fn density(count: usize, possible: usize) -> f64 {
    if possible == 0 {
        0.0
    } else {
        count as f64 / possible as f64
    }
}
