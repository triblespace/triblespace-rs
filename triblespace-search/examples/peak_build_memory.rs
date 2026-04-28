//! Measures peak heap allocation through `BM25Builder::build` and
//! `SuccinctBM25Index::to_bytes` using a process-global tracking
//! allocator.
//!
//! What the numbers reveal:
//! - Build peaks (naive and succinct) are dominated by the
//!   `term_to_tfs: HashMap<RawValue, HashMap<u32, u32>>`
//!   accumulator — at 50 k docs / 20 k vocab that's ~150 MiB
//!   alone. The streaming `SuccinctPostings::build_with` refactor
//!   trims a smaller-but-real `Vec<Vec<(u32, f32)>>` intermediate
//!   that is *masked* by `term_to_tfs` at modest scales; the win
//!   becomes visible at 100 k+ docs where the intermediate hits
//!   ~144 MiB and matters versus the HashMap's ~360 MiB.
//! - `to_bytes` peak is the cleaner before/after window: with the
//!   streaming refactor it bottoms out near the SB25 blob size
//!   (no `Vec<u8>` term re-collection, no `Vec<Vec<...>>` posting
//!   re-collection, no triple-copy handle round-trip) — the peak
//!   you see *is* effectively the output buffer.
//!
//! Methodology: each measured phase resets `PEAK` to the current
//! resident allocation, runs the operation, then reports
//! `PEAK - baseline` — the additional allocation peak the phase
//! introduced. Allocations the operation returns (the index value
//! itself, the bytes Vec) stay in the resident set and contribute
//! to the peak; intermediate work that frees before the operation
//! returns inflates only the peak, not the resident.
//!
//! Usage: `cargo run --release --example peak_build_memory`.
//!
//! Not a regression test — `tests/scale_smoke.rs` covers byte-
//! identity. This is for eyeballing the streaming optimizations'
//! effect on memory at a glance.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use triblespace_core::id::{Id, RawId};
use triblespace_search::bm25::BM25Builder;
use triblespace_search::tokens::hash_tokens;

// ── Tracking allocator ───────────────────────────────────────────
//
// `Relaxed` everywhere — we only need monotonic peak tracking, no
// happens-before with the program logic. `compare_exchange_weak`
// in a CAS loop handles concurrent updates from background threads
// (e.g., `BM25Builder::build_naive_with_threads(n)`).

struct TrackingAlloc;

static CURR: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let sz = layout.size();
        let new = CURR.fetch_add(sz, Ordering::Relaxed) + sz;
        let mut peak = PEAK.load(Ordering::Relaxed);
        while peak < new {
            match PEAK.compare_exchange_weak(peak, new, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        CURR.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static A: TrackingAlloc = TrackingAlloc;

/// Measure peak allocation delta during `f`. The returned peak is
/// `max(extra) - baseline`, where `extra` is the resident set
/// during `f`. If `f` returns a value that holds memory, that
/// memory is included in the peak.
fn measure<F, R>(label: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let baseline = CURR.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let t0 = Instant::now();
    let result = f();
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let peak = PEAK.load(Ordering::Relaxed);
    println!(
        "  {label:<40}  peak +{:>8}  ({elapsed_ms:>6.0} ms)",
        fmt_bytes(peak - baseline),
    );
    result
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

// ── Corpus ───────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

fn id_from_u64(n: u64) -> Id {
    let n = n.max(1);
    let mut raw: RawId = [0; 16];
    raw[..8].copy_from_slice(&n.to_le_bytes());
    raw[8..].copy_from_slice(&n.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
    Id::new(raw).unwrap()
}

fn fake_doc(rng: &mut Rng, vocab: usize, n_words: usize) -> String {
    let mut words = Vec::with_capacity(n_words);
    for _ in 0..n_words {
        let r = rng.next() as f64 / u64::MAX as f64;
        // Zipf-ish skew — squaring keeps common terms common.
        let biased = r * r;
        let idx = ((biased * vocab as f64) as usize).min(vocab - 1);
        words.push(format!("w{idx}"));
    }
    words.join(" ")
}

fn run(n_docs: usize, vocab: usize, doc_len: usize) {
    println!(
        "\n── n_docs = {n_docs}, vocab = {vocab}, avg_doc_len = {doc_len} ──"
    );

    let mut rng = Rng(0xC0FFEE + n_docs as u64);
    let docs: Vec<(u64, String)> = (0..n_docs)
        .map(|i| (i as u64 + 1, fake_doc(&mut rng, vocab, doc_len)))
        .collect();

    // Build a builder fresh each time so insert-time accumulation
    // doesn't carry across measurements. The closure also drops
    // the `Vec<(Value<D>, Vec<Value<T>>)>` insert-arg buffers as
    // they fall out of scope inside `insert`.
    let fresh_builder = || {
        let mut b = BM25Builder::new();
        for (id_u64, doc) in &docs {
            b.insert(id_from_u64(*id_u64), hash_tokens(doc));
        }
        b
    };

    // Naive (reference) build — keeps `term_to_tfs` HashMap-of-
    // HashMap plus the postings Vec<(u32, f32)> in memory.
    let naive = measure("BM25Builder::build_naive (reference)", || {
        fresh_builder().build_naive()
    });

    // Direct-to-succinct build via streaming SuccinctPostings::
    // build_with — the path the previous two commits restructured.
    // Peak should be smaller than the naive intermediate plus the
    // returned succinct index.
    let succinct = measure("BM25Builder::build (streaming succinct)", || {
        fresh_builder().build()
    });

    // to_bytes goes through SuccinctPostings::build_with on the
    // re-serialization side too — peak is the sum of body regions
    // plus the output buffer, no triple-allocation pattern.
    let succinct_bytes = measure("SuccinctBM25Index::to_bytes (streaming)", || {
        succinct.to_bytes()
    });

    println!(
        "  naive byte_size = {}, SB25 blob = {}",
        fmt_bytes(naive.byte_size()),
        fmt_bytes(succinct_bytes.len()),
    );
}

fn main() {
    println!("triblespace-search peak build memory");
    println!("(tracking allocator: peak = max resident during phase)");

    // Three corpus sizes — small / medium / large enough that the
    // streaming wins are visible without burning many seconds.
    run(1_000, 500, 32);
    run(10_000, 5_000, 96);
    run(50_000, 20_000, 96);
}
