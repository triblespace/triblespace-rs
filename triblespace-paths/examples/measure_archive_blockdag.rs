//! Read-only staged measurement of `block::previous+` over the local archive pile.
//!
//! The example walks the archive branch's linear commit history using commit
//! metadata, then scans only the oldest 1/2/4/8/16/32 content blobs. Content is
//! kept mmap-backed as `SimpleArchive` bytes: unrelated tribles never enter a
//! `TribleSet` or `PathIndex`.
//!
//! Run from the workspace root with:
//!
//! ```text
//! cargo run -p triblespace-paths --release --example measure_archive_blockdag
//! ```
//!
//! `PATHS_BLOCKDAG_PILE` may name another copy of the same pile. Conservative
//! pre-build limits can be changed deliberately with
//! `PATHS_BLOCKDAG_MAX_EDGES`, `PATHS_BLOCKDAG_MAX_VERTICES`, and
//! `PATHS_BLOCKDAG_MAX_DENSE_PAIRS`. A limit stops the sweep before the next
//! `PathIndex` is constructed.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::id::{Id, RawId};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::*;
use triblespace_core::query::{
    Binding, Constraint, ProposalBuffer, ProposeCursor, Variable, VariableId,
};
use triblespace_core::repo::pile::{Pile, PileReader};
use triblespace_core::repo::{self, PinStore};
use triblespace_core::trible::{Trible, TribleSet, TRIBLE_LEN};
use triblespace_paths::{Automaton, GraphEdge, PathIndex, Step, Transition};

type CommitHandle = Inline<Handle<SimpleArchive>>;

const ARCHIVE_BRANCH_ID: &str = "2C5B9785352E962368F3089A9CAEA386";
const BLOCK_PREVIOUS: RawId = [
    0x9B, 0x8F, 0x69, 0x3B, 0xE9, 0x59, 0x13, 0x6E, 0x90, 0xC3, 0x4C, 0xF0, 0x54, 0xF9, 0x03, 0x3F,
];
const RUNGS: [usize; 6] = [1, 2, 4, 8, 16, 32];

// With two automaton states the vertex default bounds the dense product
// relation to 4,194,304 pairs. The actual `previous+` relation should be much
// sparser, but the sweep does not rely on that assumption for admission.
const DEFAULT_MAX_EDGES: usize = 4_096;
const DEFAULT_MAX_VERTICES: usize = 1_024;
const DEFAULT_MAX_DENSE_PAIRS: usize = 4_194_304;

#[derive(Debug)]
struct Segment {
    content_tribles: usize,
    edges: Vec<GraphEdge>,
}

#[derive(Clone, Copy, Debug)]
struct Limits {
    edges: usize,
    vertices: usize,
    dense_pairs: usize,
}

#[derive(Debug)]
struct CapReached(String);

impl std::fmt::Display for CapReached {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CapReached {}

fn main() -> Result<(), Box<dyn Error>> {
    let pile_path = std::env::var_os("PATHS_BLOCKDAG_PILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../archive-blockdag.pile"));
    let limits = Limits {
        edges: env_limit("PATHS_BLOCKDAG_MAX_EDGES", DEFAULT_MAX_EDGES)?,
        vertices: env_limit("PATHS_BLOCKDAG_MAX_VERTICES", DEFAULT_MAX_VERTICES)?,
        dense_pairs: env_limit("PATHS_BLOCKDAG_MAX_DENSE_PAIRS", DEFAULT_MAX_DENSE_PAIRS)?,
    };

    println!("archive block-DAG path measurement (read-only, single-shot timings)");
    println!("pile             : {}", pile_path.display());
    println!("rungs            : {RUNGS:?} oldest content commits");
    println!(
        "pre-build limits : edges={} vertices={} dense_product_pairs={}",
        limits.edges, limits.vertices, limits.dense_pairs
    );

    let (reader, head) = open_reader_and_head(&pile_path)?;
    let chain = content_chain(&reader, head)?;
    let required = *RUNGS.last().expect("rungs are nonempty");
    if chain.len() < required {
        return Err(format!(
            "archive branch has only {} content commits; {required} are required",
            chain.len()
        )
        .into());
    }
    println!("content commits  : {}", chain.len());

    let automaton = previous_one_or_more();
    let mut segments = Vec::<Segment>::with_capacity(required);
    let mut leaves = Vec::<PathIndex>::with_capacity(required);
    let mut leaf_times = Vec::<Duration>::with_capacity(required);
    let mut vertices = BTreeSet::new();
    let mut matched_edges = 0usize;

    'rungs: for rung in RUNGS {
        while segments.len() < rung {
            let content = chain[segments.len()];
            match scan_segment(&reader, content, &mut matched_edges, &mut vertices, limits) {
                Ok(segment) => segments.push(segment),
                Err(error) => {
                    if let Some(cap) = error.downcast_ref::<CapReached>() {
                        println!(
                            "STOP before rung {rung}: {cap}; no PathIndex was constructed for this rung"
                        );
                        break 'rungs;
                    }
                    return Err(error);
                }
            }
        }

        let product_points = vertices
            .len()
            .checked_mul(automaton.state_count() as usize)
            .ok_or("product-point count overflow")?;
        let dense_pairs = product_points
            .checked_mul(product_points)
            .ok_or("dense product-pair count overflow")?;
        if dense_pairs > limits.dense_pairs {
            println!(
                "STOP before rung {rung}: dense product-pair bound {dense_pairs} exceeds cap {}; no PathIndex was constructed for this rung",
                limits.dense_pairs
            );
            break;
        }

        let first_new_leaf = leaves.len();
        for segment in &segments[first_new_leaf..rung] {
            let started = Instant::now();
            leaves.push(PathIndex::from_edges(
                automaton.clone(),
                segment.edges.iter().copied(),
            ));
            leaf_times.push(started.elapsed());
        }

        let merge_started = Instant::now();
        let merged = PathIndex::merge_all(leaves[..rung].iter())?;
        let merge_time = merge_started.elapsed();
        let metrics = merged.metrics();
        let stats = merged.build_stats();
        let content_tribles = segments[..rung]
            .iter()
            .map(|segment| segment.content_tribles)
            .sum::<usize>();
        let leaf_time = leaf_times[..rung].iter().copied().sum::<Duration>();

        println!(
            "rung={rung:>2} content_tribles={content_tribles:<8} previous_edges={matched_edges:<5} vertices={:<5} dense_bound={dense_pairs:<9} leaf_ms={:<9.3} merge_ms={:<9.3} product_pairs={:<9} accepted_pairs={:<9} insertions={:<8} pairs_added={:<9} largest_rectangle={} rectangle_cells={}",
            metrics.vertices,
            millis(leaf_time),
            millis(merge_time),
            metrics.product_pairs,
            metrics.accepted_pairs,
            stats.effective_insertions,
            stats.pairs_added,
            stats.largest_rectangle,
            stats.rectangle_cells_considered,
        );
        println!(
            "        rectangle_log2={}",
            format_rectangle_histogram(&stats.rectangle_log2_counts, &stats.rectangle_log2_cells,)
        );
        println!(
            "        batch components={} dag_edges={} closure_words={} accepted_canonical_bytes={} accepted_accelerator_bytes={} word_ors={} phase_ms(setup/scc/propagation/pair_count/accepted_csr)={:.3}/{:.3}/{:.3}/{:.3}/{:.3}",
            stats.batch_components,
            stats.batch_condensation_edges,
            stats.batch_bitset_words,
            stats.accepted_canonical_bytes,
            stats.accepted_accelerator_bytes,
            stats.batch_word_ors,
            stats.batch_setup_ns as f64 / 1_000_000.0,
            stats.batch_scc_ns as f64 / 1_000_000.0,
            stats.batch_propagation_ns as f64 / 1_000_000.0,
            stats.batch_pair_count_ns as f64 / 1_000_000.0,
            stats.projection_ns as f64 / 1_000_000.0,
        );
        report_query_views(&merged);
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct CandidateTimings {
    count: usize,
    estimate_us: f64,
    propose_us: f64,
    ttfr_us: f64,
}

fn report_query_views(index: &PathIndex) {
    let Some((source, target)) = index.accepted_pairs().next() else {
        println!("        views=(empty accepted relation)");
        return;
    };
    let start = Variable::<UnknownInline>::new(0);
    let end = Variable::<UnknownInline>::new(1);
    let relation = index.constraint(start, end);
    let diagonal = index.constraint(start, start);

    let unbound = Binding::default();
    let mut source_bound = Binding::default();
    source_bound.set(start.index, &source);
    let mut target_bound = Binding::default();
    target_bound.set(end.index, &target);

    let contains_us = median_us(|| {
        std::hint::black_box(index.contains(&source, &target));
    });
    let forward = candidate_timings(&relation, end.index, &source_bound);
    let reverse = candidate_timings(&relation, start.index, &target_bound);
    let starts = candidate_timings(&relation, start.index, &unbound);
    let ends = candidate_timings(&relation, end.index, &unbound);
    let diagonal = candidate_timings(&diagonal, start.index, &unbound);

    println!("        query_us contains_hit={contains_us:.3}");
    for (name, timing) in [
        ("forward", forward),
        ("reverse", reverse),
        ("starts", starts),
        ("ends", ends),
        ("diagonal", diagonal),
    ] {
        println!(
            "        query_us {name:<8} count={:<5} estimate={:<9.3} propose={:<9.3} ttfr={:.3}",
            timing.count, timing.estimate_us, timing.propose_us, timing.ttfr_us,
        );
    }
}

fn candidate_timings<'a, C: Constraint<'a>>(
    constraint: &C,
    variable: VariableId,
    binding: &Binding,
) -> CandidateTimings {
    let count = constraint
        .estimate(variable, binding)
        .expect("the measured variable belongs to the path constraint");
    let estimate_us = median_us(|| {
        std::hint::black_box(constraint.estimate(variable, binding));
    });

    let mut proposals = ProposalBuffer::new();
    let propose_us = median_us(|| {
        proposals.clear();
        constraint.propose(variable, binding, &mut proposals);
        std::hint::black_box(proposals.len());
    });
    let ttfr_us = median_us(|| {
        proposals.clear();
        let mut cursor = ProposeCursor::default();
        std::hint::black_box(constraint.propose_chunk(
            variable,
            binding,
            &mut cursor,
            1,
            &mut proposals,
        ));
    });

    CandidateTimings {
        count,
        estimate_us,
        propose_us,
        ttfr_us,
    }
}

fn median_us(mut measured: impl FnMut()) -> f64 {
    const REPETITIONS: usize = 9;
    let mut samples = [0u128; REPETITIONS];
    measured();
    for sample in &mut samples {
        let started = Instant::now();
        measured();
        *sample = started.elapsed().as_nanos();
    }
    samples.sort_unstable();
    samples[REPETITIONS / 2] as f64 / 1_000.0
}

/// Open the original pile without constructing a `Repository` (which would
/// append metadata). The returned reader owns an immutable mmap snapshot, so
/// the writable `Pile` handle can be closed immediately.
fn open_reader_and_head(path: &Path) -> Result<(PileReader, CommitHandle), Box<dyn Error>> {
    let mut pile = Pile::open(path)?;
    pile.refresh()?;
    let branch_id = Id::from_hex(ARCHIVE_BRANCH_ID).ok_or("invalid archive branch id")?;
    let branch_metadata = pile
        .head(branch_id)?
        .ok_or("archive branch id is not pinned in this pile")?;
    let reader = pile.reader()?;
    pile.close()?;

    let metadata: TribleSet = reader.get(branch_metadata)?;
    let heads = find!(
        (head: Inline<Handle<SimpleArchive>>),
        pattern!(&metadata, [{ repo::head: ?head }])
    )
    .map(|(head,)| head)
    .collect::<Vec<_>>();
    let [head] = heads.as_slice() else {
        return Err(format!(
            "archive branch metadata has {} heads; expected exactly one",
            heads.len()
        )
        .into());
    };
    Ok((reader, *head))
}

/// Walk the linear branch oldest-first and retain only content handles. Commit
/// metadata is small; content facts stay in their mmap-backed blobs.
fn content_chain(
    reader: &PileReader,
    head: CommitHandle,
) -> Result<Vec<CommitHandle>, Box<dyn Error>> {
    let mut newest_first = Vec::<Option<CommitHandle>>::new();
    let mut visited = BTreeSet::new();
    let mut cursor = Some(head);
    while let Some(handle) = cursor {
        if !visited.insert(handle) {
            return Err("archive branch commit chain contains a cycle".into());
        }
        let metadata: TribleSet = reader.get(handle)?;
        let parents = find!(
            (parent: Inline<Handle<SimpleArchive>>),
            pattern!(&metadata, [{ repo::parent: ?parent }])
        )
        .map(|(parent,)| parent)
        .collect::<Vec<_>>();
        cursor = match parents.as_slice() {
            [] => None,
            [parent] => Some(*parent),
            _ => return Err("archive branch is not a linear commit chain".into()),
        };

        let contents = find!(
            (content: Inline<Handle<SimpleArchive>>),
            pattern!(&metadata, [{ repo::content: ?content }])
        )
        .map(|(content,)| content)
        .collect::<Vec<_>>();
        match contents.as_slice() {
            [] => newest_first.push(None),
            [content] => newest_first.push(Some(*content)),
            _ => return Err("commit has more than one content blob".into()),
        }
    }

    newest_first.reverse();
    Ok(newest_first.into_iter().flatten().collect())
}

/// Scan one mmap-backed SimpleArchive and retain only `block::previous`.
/// Limits are checked as each matching fact is encountered, before the caller
/// can construct another path index.
fn scan_segment(
    reader: &PileReader,
    content: CommitHandle,
    matched_edges: &mut usize,
    vertices: &mut BTreeSet<triblespace_core::inline::RawInline>,
    limits: Limits,
) -> Result<Segment, Box<dyn Error>> {
    let blob: Blob<SimpleArchive> = reader.get(content)?;
    let mut chunks = blob.bytes.as_ref().chunks_exact(TRIBLE_LEN);
    let mut edges = Vec::new();

    for chunk in chunks.by_ref() {
        if chunk[16..32] != BLOCK_PREVIOUS {
            continue;
        }
        *matched_edges = matched_edges
            .checked_add(1)
            .ok_or("matching-edge count overflow")?;
        if *matched_edges > limits.edges {
            return Err(Box::new(CapReached(format!(
                "matching edge count {} exceeds cap {}",
                *matched_edges, limits.edges
            ))));
        }

        let raw: &[u8; TRIBLE_LEN] = chunk
            .try_into()
            .expect("chunks_exact yields one complete trible");
        let trible = Trible::as_transmute_force_raw(raw)
            .ok_or("invalid entity or attribute in validated SimpleArchive")?;
        let edge = GraphEdge::from(trible);
        vertices.insert(edge.source);
        vertices.insert(edge.target);
        if vertices.len() > limits.vertices {
            return Err(Box::new(CapReached(format!(
                "distinct vertex count {} exceeds cap {}",
                vertices.len(),
                limits.vertices
            ))));
        }
        edges.push(edge);
    }
    if !chunks.remainder().is_empty() {
        return Err("SimpleArchive content length is not a multiple of 64".into());
    }

    Ok(Segment {
        content_tribles: blob.bytes.len() / TRIBLE_LEN,
        edges,
    })
}

fn previous_one_or_more() -> Automaton {
    Automaton::new(
        2,
        [0],
        [1],
        [
            Transition::new(0, 1, Step::Forward(BLOCK_PREVIOUS)),
            Transition::new(1, 1, Step::Forward(BLOCK_PREVIOUS)),
        ],
    )
    .expect("fixed previous+ automaton is valid")
}

fn env_limit(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(default);
    };
    let raw = raw
        .into_string()
        .map_err(|_| format!("{name} is not valid UTF-8"))?;
    let value = raw
        .parse::<usize>()
        .map_err(|error| format!("{name} must be a positive integer: {error}"))?;
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
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
