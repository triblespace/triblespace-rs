//! Query-latency micro-bench.
//!
//! Builds BM25 + HNSW indexes at a handful of scales and times
//! single- and multi-term queries against both the naive and the
//! succinct views. Prints average ns / query plus p50/p99.
//!
//! Not a statistical benchmark — there's no warm-up filtering or
//! outlier rejection. Meant for "does the shape look right"
//! eyeballing against DESIGN.md's back-of-envelope claims, same
//! spirit as `blob_sizes_at_scale`.
//!
//! ```sh
//! cargo run --release --example query_latency
//! ```

use std::time::Instant;

use triblespace_core::id::{Id, RawId};
use triblespace_core::value::Inline;
use triblespace_search::bm25::BM25Builder;
use triblespace_search::hnsw::HNSWBuilder;
use triblespace_search::succinct::SuccinctHNSWIndex;
use triblespace_search::testing::BM25Index;
use triblespace_search::tokens::WordHash;
use triblespace_search::tokens::hash_tokens;

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
        let biased = r * r;
        let idx = ((biased * vocab as f64) as usize).min(vocab - 1);
        words.push(format!("w{idx}"));
    }
    words.join(" ")
}

/// Percentiles on a sorted sample of latency values (nanoseconds).
fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fmt_ns(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else {
        format!("{ns} ns")
    }
}

fn bench_bm25(n_docs: usize, vocab: usize, doc_len: usize) {
    let mut rng = Rng(0xFEE1_DEAD + n_docs as u64);
    let mut builder: BM25Builder = BM25Builder::new();
    for i in 0..n_docs {
        let doc = fake_doc(&mut rng, vocab, doc_len);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    let naive = builder.clone().build_naive();
    let succinct = builder.build();

    // Sample term-hashes at a few frequency strata. Drawing
    // queries from the same generator biases toward common
    // terms — exactly what a BM25 caller's cache is likely to
    // hit first.
    let queries: Vec<Inline<WordHash>> = (0..200)
        .map(|i| {
            let r = ((i * 37) as f64 / 200.0).powi(2);
            let idx = (r * vocab as f64) as usize;
            hash_tokens(&format!("w{}", idx.min(vocab - 1)))[0]
        })
        .collect();

    // Warm-up — populate caches.
    for q in &queries {
        let _: Vec<_> = naive.query_term(q).collect();
        let _: Vec<_> = succinct.query_term(q).collect();
    }

    let time_single = |tag: &str, f: &dyn Fn(&Inline<WordHash>)| {
        let reps = 10;
        let mut samples: Vec<u128> = Vec::with_capacity(queries.len() * reps);
        for _ in 0..reps {
            for q in &queries {
                let t0 = Instant::now();
                f(q);
                samples.push(t0.elapsed().as_nanos());
            }
        }
        samples.sort_unstable();
        let avg = samples.iter().sum::<u128>() / samples.len() as u128;
        println!(
            "  {tag:<14} avg {:<9}  p50 {:<9}  p99 {:<9}  (n={})",
            fmt_ns(avg),
            fmt_ns(percentile(&samples, 0.5)),
            fmt_ns(percentile(&samples, 0.99)),
            samples.len(),
        );
    };

    println!("BM25 single-term query  [n={n_docs}, vocab={vocab}, avg_len={doc_len}]:");
    time_single("naive", &|q| {
        let _: Vec<_> = naive.query_term(q).collect();
    });
    time_single("SB25", &|q| {
        let _: Vec<_> = succinct.query_term(q).collect();
    });

    // 3-term OR query via query_multi (naive only — succinct
    // doesn't currently expose query_multi; sum-of-query_term is
    // equivalent).
    let multi_reps = 10;
    let tri_queries: Vec<Vec<Inline<WordHash>>> = (0..100)
        .map(|i| {
            vec![
                hash_tokens(&format!("w{}", i * 3 % vocab))[0],
                hash_tokens(&format!("w{}", (i * 7 + 1) % vocab))[0],
                hash_tokens(&format!("w{}", (i * 11 + 2) % vocab))[0],
            ]
        })
        .collect();
    // Warm
    for q in &tri_queries {
        let _ = naive.query_multi(q);
    }
    let mut samples: Vec<u128> = Vec::new();
    for _ in 0..multi_reps {
        for q in &tri_queries {
            let t0 = Instant::now();
            let _ = naive.query_multi(q);
            samples.push(t0.elapsed().as_nanos());
        }
    }
    samples.sort_unstable();
    let avg = samples.iter().sum::<u128>() / samples.len() as u128;
    println!(
        "  {:<14} avg {:<9}  p50 {:<9}  p99 {:<9}  (n={})",
        "naive 3-term",
        fmt_ns(avg),
        fmt_ns(percentile(&samples, 0.5)),
        fmt_ns(percentile(&samples, 0.99)),
        samples.len(),
    );
    // Keep the compiler honest about the built indexes.
    let _ = naive.doc_count();
    let _: &BM25Index = &naive;
    let _ = succinct.doc_count();
}

fn bench_hnsw(n_docs: usize, dim: usize) {
    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::value::schemas::hash::Handle;
    use triblespace_core::value::Inline;
    use triblespace_search::schemas::{put_embedding, Embedding};

    let mut rng = Rng(0xBAD_F00D + n_docs as u64);
    let mut store = MemoryBlobStore::new();
    let mut builder = HNSWBuilder::new(dim).with_seed(13);
    let mut handles: Vec<Inline<Handle<Embedding>>> = Vec::with_capacity(n_docs);
    for _ in 0..n_docs {
        let v: Vec<f32> = (0..dim)
            .map(|_| (rng.next() as i32 as f32) / (i32::MAX as f32))
            .collect();
        let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
        builder.insert(h, v).unwrap();
        handles.push(h);
    }
    let naive = builder.build_naive();
    let succinct = SuccinctHNSWIndex::from_naive(&naive).unwrap();
    let reader = store.reader().unwrap();

    // 100 probe handles sampled from the corpus. Probing from a
    // handle rather than a raw vector is the new API; callers
    // would typically put their query vector into the store,
    // then probe from the resulting handle.
    let probes: Vec<Inline<Handle<Embedding>>> = (0..100)
        .map(|i| handles[(i * 37 + 1) % handles.len()])
        .collect();

    let naive_view = naive.attach(&reader).with_ef_search(50);
    let succinct_view = succinct.attach(&reader).with_ef_search(50);
    for p in &probes {
        let _ = naive_view.candidates_above(*p, 0.5);
        let _ = succinct_view.candidates_above(*p, 0.5);
    }

    let time = |tag: &str, f: &dyn Fn(&Inline<Handle<Embedding>>)| {
        let reps = 5;
        let mut samples: Vec<u128> = Vec::with_capacity(probes.len() * reps);
        for _ in 0..reps {
            for p in &probes {
                let t0 = Instant::now();
                f(p);
                samples.push(t0.elapsed().as_nanos());
            }
        }
        samples.sort_unstable();
        let avg = samples.iter().sum::<u128>() / samples.len() as u128;
        println!(
            "  {tag:<14} avg {:<9}  p50 {:<9}  p99 {:<9}  (n={})",
            fmt_ns(avg),
            fmt_ns(percentile(&samples, 0.5)),
            fmt_ns(percentile(&samples, 0.99)),
            samples.len(),
        );
    };

    println!("HNSW threshold query, cos ≥ 0.5, ef=50  [n={n_docs}, dim={dim}]:");
    time("naive", &|p| {
        let _ = naive_view.candidates_above(*p, 0.5);
    });
    time("SH25", &|p| {
        let _ = succinct_view.candidates_above(*p, 0.5);
    });
}

fn main() {
    bench_bm25(10_000, 2_000, 64);
    println!();
    bench_bm25(50_000, 5_000, 96);
    println!();
    bench_hnsw(5_000, 32);
    println!();
    bench_hnsw(10_000, 32);
}
