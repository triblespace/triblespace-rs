//! Read-only scale probe for `block::previous+` over the local archive pile.
//!
//! The probe walks the archive branch's linear commit history and scans only
//! the oldest selected content blobs. Content stays mmap-backed as
//! `SimpleArchive` bytes: unrelated tribles never enter a `TribleSet` or path
//! index. It measures the three public construction stages independently:
//! leaf [`PathSummary`] lowering, canonical summary union, and
//! [`PathIndex::from_summary`].
//!
//! Before closing a summary, the probe bounds the endpoint-quotient propagation
//! matrix by
//!
//! ```text
//! product_points * ceil(vertices / 64) * 8 bytes
//! ```
//!
//! because the SCC count cannot exceed the product carrier. This replaces the
//! obsolete full-product `product_points^2` guard: the production algorithm
//! propagates accepting graph endpoints, not product points. The vertex cap is
//! also a backstop for the accepted endpoint CSR, whose denotation can still be
//! quadratic on adversarial graphs.
//!
//! Run from the workspace root with:
//!
//! ```text
//! cargo run -p triblespace-paths --release --example measure_archive_blockdag
//! ```
//!
//! `PATHS_BLOCKDAG_PILE` may name another copy of the same pile. The sweep can
//! be constrained with `PATHS_BLOCKDAG_MAX_RUNG`, `PATHS_BLOCKDAG_MAX_EDGES`,
//! `PATHS_BLOCKDAG_MAX_VERTICES`, and `PATHS_BLOCKDAG_MAX_SCRATCH_BYTES`.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::id::{Id, RawId};
use triblespace_core::inline::encodings::hash::{Blake3, Handle};
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::*;
use triblespace_core::query::{
    Binding, Constraint, ProposalBuffer, ProposeCursor, Variable, VariableId,
};
use triblespace_core::repo::pile::{Pile, PileReader};
use triblespace_core::repo::{self, PinStore};
use triblespace_core::trible::{Trible, TribleSet, TRIBLE_LEN};
use triblespace_paths::{Automaton, GraphEdge, PathIndex, PathSummary, Step, Transition};

type CommitHandle = Inline<Handle<SimpleArchive>>;

const ARCHIVE_BRANCH_ID: &str = "2C5B9785352E962368F3089A9CAEA386";
const BLOCK_PREVIOUS: RawId = [
    0x9B, 0x8F, 0x69, 0x3B, 0xE9, 0x59, 0x13, 0x6E, 0x90, 0xC3, 0x4C, 0xF0, 0x54, 0xF9, 0x03, 0x3F,
];
const RUNGS: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

// These defaults admit the measured 12,170-vertex rung 64 and stop before the
// next steep growth. At the vertex cap, even an adversarially full forward and
// reverse endpoint CSR would retain about 1.26 GiB of u32 values; the measured
// rung 64 is far smaller. The limits can be raised deliberately on a larger
// machine.
const DEFAULT_MAX_RUNG: usize = 128;
const DEFAULT_MAX_EDGES: usize = 20_000;
const DEFAULT_MAX_VERTICES: usize = 13_000;
const DEFAULT_MAX_SCRATCH_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug)]
struct Segment {
    content_tribles: usize,
    edges: Vec<GraphEdge>,
}

#[derive(Clone, Copy, Debug)]
struct Limits {
    max_rung: usize,
    edges: usize,
    vertices: usize,
    scratch_bytes: usize,
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
        max_rung: env_limit("PATHS_BLOCKDAG_MAX_RUNG", DEFAULT_MAX_RUNG)?,
        edges: env_limit("PATHS_BLOCKDAG_MAX_EDGES", DEFAULT_MAX_EDGES)?,
        vertices: env_limit("PATHS_BLOCKDAG_MAX_VERTICES", DEFAULT_MAX_VERTICES)?,
        scratch_bytes: env_limit(
            "PATHS_BLOCKDAG_MAX_SCRATCH_BYTES",
            DEFAULT_MAX_SCRATCH_BYTES,
        )?,
    };

    println!("archive block-DAG endpoint-quotient scale probe (read-only)");
    println!("pile             : {}", pile_path.display());
    println!("rungs            : {RUNGS:?} (maximum {})", limits.max_rung);
    println!(
        "pre-build limits : edges={} vertices={} endpoint_quotient_scratch_bytes={}",
        limits.edges, limits.vertices, limits.scratch_bytes
    );

    let (reader, head) = open_reader_and_head(&pile_path)?;
    let chain = content_chain(&reader, head)?;
    println!("content commits  : {}", chain.len());

    let automaton = previous_one_or_more();
    let mut segments = Vec::<Segment>::new();
    let mut leaves = Vec::<PathSummary>::new();
    let mut leaf_times = Vec::<Duration>::new();
    let mut vertices = BTreeSet::new();
    let mut matched_edges = 0usize;

    'rungs: for rung in RUNGS
        .iter()
        .copied()
        .take_while(|&rung| rung <= limits.max_rung)
    {
        if chain.len() < rung {
            println!(
                "STOP before rung {rung}: archive branch has only {} content commits",
                chain.len()
            );
            break;
        }

        while segments.len() < rung {
            let content = chain[segments.len()];
            match scan_segment(&reader, content, &mut matched_edges, &mut vertices, limits) {
                Ok(segment) => segments.push(segment),
                Err(error) => {
                    if let Some(cap) = error.downcast_ref::<CapReached>() {
                        println!(
                            "STOP before rung {rung}: {cap}; no summary merge or PathIndex was constructed for this rung"
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
        let row_words = vertices.len().div_ceil(u64::BITS as usize);
        let scratch_bytes = product_points
            .checked_mul(row_words)
            .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
            .ok_or("endpoint-quotient scratch bound overflow")?;
        if scratch_bytes > limits.scratch_bytes {
            println!(
                "STOP before rung {rung}: endpoint-quotient scratch bound {scratch_bytes} exceeds cap {}; no summary merge or PathIndex was constructed for this rung",
                limits.scratch_bytes
            );
            break;
        }

        while leaves.len() < rung {
            let segment = &segments[leaves.len()];
            let started = Instant::now();
            leaves.push(PathSummary::from_edges(
                automaton.clone(),
                segment.edges.iter().copied(),
            ));
            leaf_times.push(started.elapsed());
        }

        let merge_started = Instant::now();
        let merged = PathSummary::merge_all(leaves[..rung].iter())?;
        let merge_time = merge_started.elapsed();
        let merged_vertices = merged.vertices().len();
        let direct_arcs = merged.direct_arc_count();
        assert_eq!(merged_vertices, vertices.len());

        let first_input = merged.clone();
        let first_started = Instant::now();
        let first = PathIndex::from_summary(first_input)?;
        let first_close = first_started.elapsed();
        let accepted_pairs = first.accepted_pair_count();
        let signature = closure_signature(&first, direct_arcs);
        let query_report = query_views(&first);
        drop(first);

        let warm_repetitions = if rung >= 32 { 3 } else { 1 };
        let mut warm_closes = Vec::with_capacity(warm_repetitions);
        for _ in 0..warm_repetitions {
            let input = merged.clone();
            let started = Instant::now();
            let index = PathIndex::from_summary(input)?;
            let elapsed = started.elapsed();
            std::hint::black_box(&index);
            warm_closes.push(elapsed);
        }

        let content_tribles = segments[..rung]
            .iter()
            .map(|segment| segment.content_tribles)
            .sum::<usize>();
        let leaf_time = leaf_times[..rung].iter().copied().sum::<Duration>();
        let warm_millis = warm_closes
            .iter()
            .map(|duration| format!("{:.3}", millis(*duration)))
            .collect::<Vec<_>>()
            .join(",");
        let warm_median = median_duration(&warm_closes);

        println!(
            "rung={rung:>3} content_tribles={content_tribles:<9} previous_edges={matched_edges:<6} vertices={merged_vertices:<6} product_points={product_points:<6} direct_arcs={direct_arcs:<6} accepted_pairs={accepted_pairs:<10} scratch_bound_bytes={scratch_bytes:<10} leaf_lower_ms={:<9.3} summary_merge_ms={:<9.3} close_first_ms={:<9.3} close_warm_ms=[{warm_millis}] close_warm_median_ms={:<9.3} signature={signature}",
            millis(leaf_time),
            millis(merge_time),
            millis(first_close),
            millis(warm_median),
        );
        println!("        {query_report}");
    }

    Ok(())
}

fn closure_signature(index: &PathIndex, direct_arcs: usize) -> String {
    let mut hasher = Blake3::new();
    hasher.update(b"triblespace-paths/accepted-endpoint-relation/v1\0");
    for count in [
        index.automaton().state_count() as u64,
        index.vertex_count() as u64,
        direct_arcs as u64,
        index.accepted_pair_count() as u64,
    ] {
        hasher.update(&count.to_le_bytes());
    }
    for (source, target) in index.accepted_pairs() {
        hasher.update(&source);
        hasher.update(&target);
    }
    hex(&hasher.finalize())
}

#[derive(Clone, Copy, Debug)]
struct CandidateTimings {
    count: usize,
    estimate_us: f64,
    propose_us: f64,
    ttfr_us: f64,
}

fn query_views(index: &PathIndex) -> String {
    let Some((source, target)) = index.accepted_pairs().next() else {
        return "query_us=(empty accepted relation)".to_owned();
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

    format!(
        "query_us contains_hit={contains_us:.3} {} {} {} {} {}",
        format_candidates("forward", forward),
        format_candidates("reverse", reverse),
        format_candidates("starts", starts),
        format_candidates("ends", ends),
        format_candidates("diagonal", diagonal),
    )
}

fn format_candidates(name: &str, timing: CandidateTimings) -> String {
    format!(
        "{name}(count={},estimate={:.3},propose={:.3},ttfr={:.3})",
        timing.count, timing.estimate_us, timing.propose_us, timing.ttfr_us
    )
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
    const BATCH: usize = 256;
    let mut samples = [0u128; REPETITIONS];
    for _ in 0..BATCH {
        measured();
    }
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..BATCH {
            measured();
        }
        *sample = started.elapsed().as_nanos();
    }
    samples.sort_unstable();
    samples[REPETITIONS / 2] as f64 / BATCH as f64 / 1_000.0
}

fn median_duration(samples: &[Duration]) -> Duration {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// Open the pile only long enough to acquire an immutable mmap snapshot. No
/// repository is constructed and this example never writes or pins anything.
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

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    result
}
