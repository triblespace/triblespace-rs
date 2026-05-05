//! Prints the naive vs. SB25 blob size for a few fake-corpus
//! sizes, plus build time. Meant for eyeballing compression at a
//! glance — `cargo run --release --example blob_sizes_at_scale`.
//!
//! Not an assertion test; that's what the regression guards in
//! `tests/scale_smoke.rs` are for.

use std::time::Instant;

use triblespace_core::id::{Id, RawId};
use triblespace_search::bm25::BM25Builder;
use triblespace_search::tokens::hash_tokens;

/// Tiny deterministic PRNG (SplitMix64) so runs are reproducible.
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

/// A naturally correlated GenId: share 11 bytes of fixed prefix
/// (simulating "all entities minted from the same namespace seed
/// during one session") and vary only the last 5 bytes by the
/// doc index. Exposes the best case for `CompressedUniverse`'s
/// 4-byte fragment dictionary: many docs share the same leading
/// fragments.
fn correlated_id(prefix: &[u8; 11], n: u64) -> Id {
    let mut raw: RawId = [0; 16];
    raw[..11].copy_from_slice(prefix);
    // Last 5 bytes carry the doc-unique payload. `.max(1)` keeps
    // the low byte non-zero so the overall Id is never nil.
    let bytes = n.to_le_bytes();
    raw[11..16].copy_from_slice(&bytes[..5]);
    if raw.iter().all(|&b| b == 0) {
        raw[15] = 1;
    }
    Id::new(raw).unwrap()
}

fn fake_doc(rng: &mut Rng, vocab: usize, n_words: usize) -> String {
    let mut words = Vec::with_capacity(n_words);
    for _ in 0..n_words {
        let r = rng.next() as f64 / u64::MAX as f64;
        // Zipf-ish skew via squaring keeps common terms common.
        let biased = r * r;
        let idx = ((biased * vocab as f64) as usize).min(vocab - 1);
        words.push(format!("w{idx}"));
    }
    words.join(" ")
}

/// Read the keys-section length from a built index. Wraps
/// [`SuccinctBM25Index::keys_size_bytes`] so the wire-format
/// layout (where the fragment-dictionary and DacsByte payload
/// sections live inside the canonical bytes) stays inside the
/// crate.
fn keys_len_from_blob(idx: &triblespace_search::succinct::SuccinctBM25Index) -> usize {
    idx.keys_size_bytes()
}

#[derive(Clone, Copy)]
enum KeyDist {
    Scattered,
    Correlated,
}

fn bench(n_docs: usize, vocab: usize, doc_len: usize, keys: KeyDist) {
    let mut rng = Rng(0xC0FFEE + n_docs as u64);
    // Materialise the docs once and reuse across serial + parallel
    // build paths so the measured time is build-only, not doc-gen.
    let docs: Vec<(u64, String)> = (0..n_docs)
        .map(|i| (i as u64 + 1, fake_doc(&mut rng, vocab, doc_len)))
        .collect();

    // Shared 11-byte prefix drawn deterministically from the
    // bench seed so the correlated-keys case is reproducible.
    let mut prefix_rng = Rng(0xABCD_1234 + n_docs as u64);
    let mut prefix = [0u8; 11];
    for slot in prefix.iter_mut() {
        *slot = (prefix_rng.next() & 0xFF) as u8;
    }
    // Ensure the prefix isn't all zeros — that would make the
    // last-5 payload the *only* distinguishing bytes and we'd
    // accidentally measure the raw 16-byte Id case, not the
    // shared-prefix case.
    if prefix.iter().all(|&b| b == 0) {
        prefix[0] = 1;
    }

    let fresh_builder = || {
        let mut b = BM25Builder::new();
        for (id_u64, doc) in &docs {
            let id = match keys {
                KeyDist::Scattered => id_from_u64(*id_u64),
                KeyDist::Correlated => correlated_id(&prefix, *id_u64),
            };
            b.insert(id, hash_tokens(doc));
        }
        b
    };

    // Single-threaded naive build (reference — timing the scoring
    // loop on its own).
    let t0 = Instant::now();
    let naive = fresh_builder().build_naive_with_threads(1);
    let build_ms_serial = t0.elapsed().as_secs_f64() * 1000.0;

    // Parallel naive build. 4 threads is a typical laptop-class
    // sweet spot; push higher and the merge cost starts to eat
    // the win.
    let threads = 4;
    let t_par = Instant::now();
    let parallel_naive = fresh_builder().build_naive_with_threads(threads);
    let build_ms_par = t_par.elapsed().as_secs_f64() * 1000.0;
    // Bit-identical output is the load-bearing invariant.
    debug_assert_eq!(naive, parallel_naive);

    // Direct-to-succinct build (the production path).
    let t1 = Instant::now();
    let succinct = fresh_builder().build();
    let encode_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let naive_size = naive.byte_size();
    let succinct_size = succinct.bytes.len();

    let ratio = succinct_size as f64 / naive_size as f64;
    let speedup = build_ms_serial / build_ms_par;
    let keys_bytes = keys_len_from_blob(&succinct);
    // A flat `n_docs × 32 B` table is the Phase-1 baseline for
    // keys — diffing against it reports what `CompressedUniverse`
    // actually saved on this key distribution.
    let keys_flat = n_docs * 32;
    let keys_ratio = keys_bytes as f64 / keys_flat as f64;
    let dist_tag = match keys {
        KeyDist::Scattered => "scattered",
        KeyDist::Correlated => "correlated",
    };

    println!(
        "n={n_docs:>6}  keys={dist_tag:<10}  vocab={vocab:>5}  avg_doc_len={doc_len:>3} \
         | build-1 {build_ms_serial:>5.0}ms  build-{threads} {build_ms_par:>5.0}ms \
         ({speedup:>3.1}×)  succinct-encode {encode_ms:>5.0}ms \
         | naive {:>8}  SB25 {:>8}  ratio {:.2}×  keys {:>8}/{:>8} ({:.2}×)",
        fmt_bytes(naive_size),
        fmt_bytes(succinct_size),
        ratio,
        fmt_bytes(keys_bytes),
        fmt_bytes(keys_flat),
        keys_ratio,
    );
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

fn main() {
    println!("BM25 blob size: naive vs SB25 (succinct)");
    println!(
        "\"scattered\"  keys use `id_from_u64` — all 16 trailing \
         bytes vary pseudo-randomly."
    );
    println!(
        "\"correlated\" keys share an 11-byte prefix — simulates \
         one-session-minted entity ids."
    );
    println!(
        "keys column: actual / flat-32B baseline, with the \
         CompressedUniverse compression ratio."
    );
    println!(
        "-------------------------------------------------------------\
         -----------------------------------------"
    );
    for n in [1_000usize, 5_000, 10_000, 50_000] {
        let (vocab, len) = match n {
            1_000 => (400usize, 24),
            5_000 => (1_000, 48),
            10_000 => (2_000, 64),
            _ => (5_000, 96),
        };
        bench(n, vocab, len, KeyDist::Scattered);
        bench(n, vocab, len, KeyDist::Correlated);
    }
}
