//! Phase-level smoke benchmark for succinct BM25 segment merging.
//!
//! This intentionally uses only public APIs so it exercises the same
//! `Bm25Rollup::merge` path as index-home compaction. It is not a statistical
//! benchmark; run the release binary repeatedly (and under `/usr/bin/time -l`
//! on macOS) when comparing merge implementations.
//!
//! Usage:
//! `cargo run --release -p triblespace-search --example bm25_merge_bench -- \
//!      [segments] [docs_per_segment] [terms_per_doc] [vocabulary] [overlap_percent]`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::id::{Id, RawId};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::repo::index_home::IndexKind;
use triblespace_core::repo::BlobStore;
use triblespace_search::bm25::BM25Builder;
use triblespace_search::index_bm25::Bm25Rollup;
use triblespace_search::succinct::SuccinctBM25Index;
use triblespace_search::tokens::WordHash;

struct TrackingAlloc;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let next = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(next, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAlloc = TrackingAlloc;

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn id_from_u64(n: u64) -> Id {
    let mut raw: RawId = [0; 16];
    raw[0] = 1;
    raw[8..].copy_from_slice(&n.to_be_bytes());
    Id::new(raw).expect("non-zero benchmark id")
}

fn term_from_u64(n: u64) -> Inline<WordHash> {
    let mut raw: RawInline = [0; 32];
    raw[..8].copy_from_slice(&n.to_be_bytes());
    raw[8..16].copy_from_slice(&n.rotate_left(17).to_be_bytes());
    raw[16..24].copy_from_slice(&n.rotate_left(31).to_be_bytes());
    raw[24..].copy_from_slice(&n.rotate_left(47).to_be_bytes());
    Inline::new(raw)
}

fn build_segments(
    segment_count: usize,
    docs_per_segment: usize,
    terms_per_doc: usize,
    vocabulary: usize,
    overlap_percent: usize,
) -> Vec<SuccinctBM25Index<GenId, WordHash>> {
    assert!(segment_count > 0);
    assert!(vocabulary > 0);
    assert!(overlap_percent <= 100);
    let shared_docs = docs_per_segment * overlap_percent / 100;

    (0..segment_count)
        .map(|segment| {
            let mut builder: BM25Builder<GenId, WordHash> = BM25Builder::new();
            let mut rng = Rng(0xB25_5EED ^ segment as u64);
            for local in 0..docs_per_segment {
                let ordinal = if local < shared_docs {
                    local as u64
                } else {
                    shared_docs as u64
                        + (segment * (docs_per_segment - shared_docs) + local - shared_docs) as u64
                };
                let terms =
                    (0..terms_per_doc).map(|_| term_from_u64(rng.next() % vocabulary as u64));
                builder.insert(id_from_u64(ordinal + 1), terms);
            }
            builder.build()
        })
        .collect()
}

fn parse_arg(index: usize, default: usize) -> usize {
    std::env::args()
        .nth(index)
        .map(|value| value.parse().expect("benchmark arguments are usize"))
        .unwrap_or(default)
}

fn fmt_bytes(bytes: usize) -> String {
    if bytes >= 1 << 30 {
        format!("{:.2} GiB", bytes as f64 / (1_u64 << 30) as f64)
    } else if bytes >= 1 << 20 {
        format!("{:.2} MiB", bytes as f64 / (1_u64 << 20) as f64)
    } else {
        format!("{:.2} KiB", bytes as f64 / (1_u64 << 10) as f64)
    }
}

fn main() {
    let segment_count = parse_arg(1, 8);
    let docs_per_segment = parse_arg(2, 2_000);
    let terms_per_doc = parse_arg(3, 64);
    let vocabulary = parse_arg(4, 8_192);
    let overlap_percent = parse_arg(5, 10);

    let build_started = Instant::now();
    let segments = build_segments(
        segment_count,
        docs_per_segment,
        terms_per_doc,
        vocabulary,
        overlap_percent,
    );
    let input_bytes: usize = segments.iter().map(|segment| segment.bytes.len()).sum();
    println!(
        "prepared {segment_count} segments x {docs_per_segment} docs x \
         {terms_per_doc} terms ({overlap_percent}% overlap, vocab {vocabulary}) in {:.3}s; input {}",
        build_started.elapsed().as_secs_f64(),
        fmt_bytes(input_bytes),
    );

    let mut store = MemoryBlobStore::new();
    let reader = store.reader().expect("memory reader");
    let kind = Bm25Rollup::new(reader, id_from_u64(u64::MAX));

    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let merge_started = Instant::now();
    let merged_blob = kind
        .merge(&segments)
        .expect("canonical segments merge")
        .into_iter()
        .next()
        .expect("non-empty benchmark input produces one artifact");
    let elapsed = merge_started.elapsed();
    let extra_heap = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let digest = blake3::hash(merged_blob.bytes.as_ref());

    println!(
        "merge {:.3}s; heap peak +{}; output {}; blake3 {}",
        elapsed.as_secs_f64(),
        fmt_bytes(extra_heap),
        fmt_bytes(merged_blob.bytes.len()),
        digest.to_hex(),
    );
}
