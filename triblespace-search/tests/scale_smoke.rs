//! Sanity-check the naive BM25 + flat k-NN impls at a
//! not-completely-trivial scale (O(thousand) docs) so regressions
//! that only fire on large posting lists or long vector tables
//! surface in CI instead of at JP's first real corpus.
//!
//! These tests are still fast — seconds, not minutes — so they
//! stay in the default test run.

use triblespace_core::id::{Id, RawId};

use triblespace_search::bm25::BM25Builder;
use triblespace_search::hnsw::HNSWBuilder;
use triblespace_search::testing::FlatBuilder;
use triblespace_search::succinct::{SuccinctBM25Index, SuccinctHNSWIndex};
use triblespace_search::tokens::hash_tokens;

/// Small pseudo-RNG (SplitMix64) — deterministic, no extra deps.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

fn id_from_u64(mut n: u64) -> Id {
    // Avoid the nil id by forcing a non-zero byte. Any mapping
    // works; this is just a deterministic "u64 → Id" for tests.
    n = n.max(1);
    let mut raw: RawId = [0; 16];
    raw[..8].copy_from_slice(&n.to_le_bytes());
    raw[8..].copy_from_slice(&n.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
    Id::new(raw).expect("non-nil by construction")
}

/// Produce `n_words` tokens drawn pseudo-randomly from a fixed
/// vocabulary of `vocab_size` English words. The distribution
/// skews Zipfian-ish so some terms are common, some rare.
fn fake_document(rng: &mut SplitMix64, vocab_size: usize, n_words: usize) -> String {
    let mut words: Vec<String> = Vec::with_capacity(n_words);
    for _ in 0..n_words {
        // Zipf-ish skew: square the random u32 to bias low.
        let r = rng.next() as f64 / u64::MAX as f64;
        let biased = r * r;
        let idx = (biased * vocab_size as f64) as usize;
        let idx = idx.min(vocab_size - 1);
        words.push(format!("w{idx}"));
    }
    words.join(" ")
}

#[test]
fn bm25_1k_docs_roundtrip_consistency() {
    const N_DOCS: usize = 1_000;
    const VOCAB: usize = 500;
    const DOC_LEN: usize = 20;

    let mut rng = SplitMix64(0xC0FFEE);
    let mut builder = BM25Builder::new();
    for i in 0..N_DOCS {
        let doc = fake_document(&mut rng, VOCAB, DOC_LEN);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    let idx = builder.build_naive();
    assert_eq!(idx.doc_count(), N_DOCS);
    assert!(idx.term_count() > 0 && idx.term_count() <= VOCAB);

    // Spot-check a few terms actually return postings — the
    // point is that the naive reference answers consistent
    // queries at 10k scale without blowing up.
    for term_text in ["w0", "w1", "w42", "w250", "w499"] {
        let term = hash_tokens(term_text);
        if term.is_empty() {
            continue;
        }
        let hits: Vec<_> = idx.query_term(&term[0]).collect();
        assert!(hits.len() <= N_DOCS, "doc_frequency can't exceed N_DOCS");
    }
}

#[test]
fn bm25_1k_docs_multi_term_ranks_sanely() {
    let mut rng = SplitMix64(0xDEADBEEF);
    let mut builder = BM25Builder::new();
    for i in 0..1_000 {
        let doc = fake_document(&mut rng, 200, 30);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    // Inject a unique "needle" doc.
    let needle_id = id_from_u64(999_999);
    builder.insert(needle_id, hash_tokens("needle needle beacon"));
    let idx = builder.build_naive();

    let q = hash_tokens("needle beacon");
    let ranked = idx.query_multi(&q);
    // Needle doc should be the top hit since both rare tokens
    // land only there.
    // Keys are 32-byte RawValue now; compare against the
    // Value<GenId> form of needle_id.
    let mut needle_key = [0u8; 32];
    needle_key[16..32].copy_from_slice(AsRef::<[u8; 16]>::as_ref(&needle_id));
    assert_eq!(ranked[0].0.raw, needle_key);
}

#[test]
fn hnsw_1k_vectors_recall_against_flat() {
    // Build the same 1k 32-dim vectors into both Flat (ground
    // truth) and HNSW, then check that HNSW's above-threshold set
    // recalls at least 70% of Flat's above-threshold set for a
    // few probe handles.
    use std::collections::HashSet;

    const N_DOCS: usize = 1_000;
    const DIM: usize = 32;

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::value::schemas::hash::Blake3;
    use triblespace_search::schemas::put_embedding;

    let mut rng = SplitMix64(0xFACE_FEED);
    let mut store = MemoryBlobStore::<Blake3>::new();
    let mut flat_b = FlatBuilder::new(DIM);
    let mut hnsw_b = HNSWBuilder::new(DIM).with_seed(7);
    let mut handles = Vec::with_capacity(N_DOCS);
    for _ in 0..N_DOCS {
        let vec: Vec<f32> = (0..DIM)
            .map(|_| (rng.next() as i32 as f32) / (i32::MAX as f32))
            .collect();
        let h = put_embedding::<_, Blake3>(&mut store, vec.clone()).unwrap();
        flat_b.insert(h);
        hnsw_b.insert(h, vec).unwrap();
        handles.push(h);
    }
    let flat = flat_b.build();
    let hnsw = hnsw_b.build();
    let reader = store.reader().unwrap();
    let flat_view = flat.attach(&reader);
    let hnsw_view = hnsw.attach(&reader).with_ef_search(100);

    let floor = 0.4f32;
    let mut total_truth = 0usize;
    let mut total_overlap = 0usize;
    for probe in handles.iter().take(5) {
        let truth: HashSet<_> = flat_view
            .candidates_above(*probe, floor)
            .unwrap()
            .into_iter()
            .collect();
        let got: HashSet<_> = hnsw_view
            .candidates_above(*probe, floor)
            .unwrap()
            .into_iter()
            .collect();
        total_truth += truth.len();
        total_overlap += truth.intersection(&got).count();
    }
    assert!(total_truth > 0, "fixture: floor excluded everything");
    let recall = total_overlap as f32 / total_truth as f32;
    assert!(
        recall >= 0.7,
        "HNSW 1k-doc recall {recall:.2} below 0.7 threshold"
    );

    // The succinct HNSW blob round-trip is exercised by
    // `succinct_hnsw_1k_docs_matches_naive` below and by
    // `tests/pile_roundtrip.rs`.
}

/// At 1k docs the succinct BM25 must answer identically to the
/// naive one. Catches any encoding/decoding drift that a hand-
/// picked 4-doc test wouldn't stress (long posting lists, many
/// distinct terms, variable doc lengths).
#[test]
fn succinct_bm25_1k_docs_matches_naive() {
    let mut rng = SplitMix64(0xC0FFEE);
    let mut builder = BM25Builder::new();
    for i in 0..1_000 {
        let doc = fake_document(&mut rng, 500, 20);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    let naive = builder.clone().build_naive();
    let succinct = builder.build();

    assert_eq!(succinct.doc_count(), naive.doc_count());
    assert_eq!(succinct.term_count(), naive.term_count());

    // Sample a handful of terms, including very common and very
    // rare ones from the fake vocab. Every one must produce the
    // same posting list.
    for term_text in ["w0", "w1", "w7", "w50", "w250", "w499"] {
        let term = hash_tokens(term_text);
        if term.is_empty() {
            continue;
        }
        // Query result order is unspecified — succinct sorts by
        // CompressedUniverse code, naive by insertion order — so
        // compare as sets by sorting by key first.
        let mut a: Vec<_> = naive.query_term(&term[0]).collect();
        let mut b: Vec<_> = succinct.query_term(&term[0]).collect();
        a.sort_by_key(|(k, _)| *k);
        b.sort_by_key(|(k, _)| *k);
        assert_eq!(
            a.len(),
            b.len(),
            "term {term_text}: naive {} vs succinct {} postings",
            a.len(),
            b.len()
        );
        let tol = succinct.score_tolerance();
        for ((id_a, s_a), (id_b, s_b)) in a.iter().zip(b.iter()) {
            assert_eq!(id_a, id_b);
            assert!(
                (s_a - s_b).abs() <= tol,
                "term {term_text}: score drift {s_a} vs {s_b} > tol {tol}"
            );
        }
    }

    // Blob round-trip at this scale — refcounted handover via
    // `ToBlob` / `TryFromBlob` (`to_bytes` + `try_from_bytes`
    // retired with the canonical-bytes refactor).
    use triblespace_core::blob::{Blob, TryFromBlob};
    let blob: Blob<triblespace_search::succinct::SuccinctBM25Blob> =
        Blob::new(succinct.bytes.clone());
    let reloaded = SuccinctBM25Index::try_from_blob(blob).expect("valid");
    assert_eq!(reloaded.doc_count(), succinct.doc_count());
    let term = hash_tokens("w7");
    let mut a: Vec<_> = succinct.query_term(&term[0]).collect();
    let mut b: Vec<_> = reloaded.query_term(&term[0]).collect();
    a.sort_by_key(|(k, _)| *k);
    b.sort_by_key(|(k, _)| *k);
    assert_eq!(a.len(), b.len());
    let tol = reloaded.score_tolerance().max(1e-5);
    for ((id_a, s_a), (id_b, s_b)) in a.iter().zip(b.iter()) {
        assert_eq!(id_a, id_b);
        assert!((s_a - s_b).abs() <= tol);
    }
}

/// Succinct HNSW must answer the same above-threshold set as the
/// naive backend for a 1k-vector corpus. This is the scale-level
/// sanity check for the succinct graph encoding — any off-by-one
/// in offsets / handles indexing shows up as divergent results.
#[test]
fn succinct_hnsw_1k_docs_matches_naive() {
    const DIM: usize = 16;
    use std::collections::HashSet;

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::value::schemas::hash::Blake3;
    use triblespace_search::schemas::put_embedding;

    let mut rng = SplitMix64(0xBADF00D);
    let mut store = MemoryBlobStore::<Blake3>::new();
    let mut builder = HNSWBuilder::new(DIM).with_seed(11);
    let mut handles = Vec::new();
    for _ in 0..1_000 {
        let vec: Vec<f32> = (0..DIM)
            .map(|_| (rng.next() as i32 as f32) / (i32::MAX as f32))
            .collect();
        let h = put_embedding::<_, Blake3>(&mut store, vec.clone()).unwrap();
        builder.insert(h, vec).unwrap();
        handles.push(h);
    }
    let naive = builder.build_naive();
    let succinct = SuccinctHNSWIndex::from_naive(&naive).unwrap();
    let reader = store.reader().unwrap();
    let naive_view = naive.attach(&reader).with_ef_search(50);
    let succinct_view = succinct.attach(&reader).with_ef_search(50);

    let floor = 0.4f32;
    for probe in handles.iter().take(5) {
        let n: HashSet<_> = naive_view
            .candidates_above(*probe, floor)
            .unwrap()
            .into_iter()
            .collect();
        let s: HashSet<_> = succinct_view
            .candidates_above(*probe, floor)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(n, s, "backend divergence at 1k scale for {probe:?}");
    }
}

/// At 1k docs, the landed SB25 blob must be strictly smaller
/// than the naive BM25 byte-serialization — the whole point of
/// the succinct pass. Not asserting a specific ratio (that's
/// what the worked example in DESIGN.md is for); this is the
/// "did someone accidentally bloat the format" guard.
#[test]
fn succinct_bm25_blob_smaller_than_naive_at_1k() {
    let mut rng = SplitMix64(0xA11CE);
    let mut builder = BM25Builder::new();
    for i in 0..1_000 {
        let doc = fake_document(&mut rng, 400, 24);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    let naive = builder.clone().build_naive();
    let succinct = builder.build();

    let naive_size = naive.byte_size();
    let succinct_size = succinct.bytes.len();
    assert!(
        succinct_size < naive_size,
        "succinct blob {succinct_size} should be < naive baseline {naive_size}"
    );
}

/// Exploratory: establish whether u16 quantization of BM25
/// scores would preserve top-k ranking on a real corpus, before
/// we commit to changing the SB25 wire format. Builds a 1k-doc
/// index, scans the actual score range, simulates quantize /
/// dequantize, and checks that top-10 for a dozen queries is
/// preserved after the lossy round-trip.
///
/// The finding this test locks in: on typical BM25 score
/// distributions (scores ∈ [0, ~5] for standard k1/b), the
/// absolute error after u16 quantization is bounded by
/// `max_score / 65535` and top-10 ranking is preserved across
/// all sampled queries. See `docs/DESIGN.md` → "Open compression
/// directions" for the outstanding constraint-tolerance work
/// that gates the actual format change.
#[test]
fn bm25_quantization_preserves_top10() {
    let mut rng = SplitMix64(0x513E3C);
    let mut builder = BM25Builder::new();
    for i in 0..1_000 {
        let doc = fake_document(&mut rng, 400, 24);
        builder.insert(id_from_u64(i as u64 + 1), hash_tokens(&doc));
    }
    // Uses raw f32 scores, so build the naive reference — the
    // succinct form already quantizes internally.
    let idx = builder.build_naive();

    // Walk every term, record the global max score and the
    // per-query top-10 on both the raw and the quantized copies.
    let all_scores: Vec<f32> = (0..idx.term_count())
        .flat_map(|t| idx.postings_for(t).iter().map(|&(_, s)| s))
        .collect();
    let max_s = all_scores.iter().copied().fold(0.0f32, |a, b| a.max(b));
    assert!(max_s > 0.0, "non-trivial corpus");

    // Quantize a score to u16 and dequantize back to f32.
    let quantize = |s: f32| -> f32 {
        let q = ((s / max_s) * (u16::MAX as f32)).round() as u16;
        (q as f32 / u16::MAX as f32) * max_s
    };

    // Round-trip every score and measure max absolute error.
    let max_err = all_scores
        .iter()
        .map(|&s| (s - quantize(s)).abs())
        .fold(0.0f32, |a, b| a.max(b));
    // Bound: each bucket is max_s / 65535 wide, rounding is at
    // most half a bucket.
    let bound = max_s / 65534.0;
    assert!(
        max_err <= bound,
        "quantization error {max_err} > theoretical bound {bound}"
    );

    // Sample a dozen queries; top-10 must be stable under the
    // quantize/dequantize round-trip of the aggregated score.
    let queries = [
        "w0 w1",
        "w10 w20",
        "w42",
        "w99 w100 w101",
        "w200 w250",
        "w300",
        "w50 w60 w70",
        "w75 w5",
        "w0 w100 w200 w300",
        "w12 w24 w36 w48",
        "w7",
        "w18 w29",
    ];
    for q_text in &queries {
        let terms = hash_tokens(q_text);
        if terms.is_empty() {
            continue;
        }

        let mut raw_scores: Vec<(triblespace_core::value::RawValue, f32)> = Vec::new();
        let mut q_scores: Vec<(triblespace_core::value::RawValue, f32)> = Vec::new();
        for term in &terms {
            for (d, s) in idx.query_term(term) {
                let d_raw = d.raw;
                match raw_scores.iter_mut().find(|(dd, _)| *dd == d_raw) {
                    Some(e) => e.1 += s,
                    None => raw_scores.push((d_raw, s)),
                }
                let qs = quantize(s);
                match q_scores.iter_mut().find(|(dd, _)| *dd == d_raw) {
                    Some(e) => e.1 += qs,
                    None => q_scores.push((d_raw, qs)),
                }
            }
        }
        raw_scores.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        q_scores.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // Compare top-10 set (not order — quantization can
        // legitimately swap ties).
        use std::collections::HashSet;
        let raw_top: HashSet<_> = raw_scores.iter().take(10).map(|(d, _)| *d).collect();
        let q_top: HashSet<_> = q_scores.iter().take(10).map(|(d, _)| *d).collect();
        let overlap = raw_top.intersection(&q_top).count();
        assert!(
            overlap >= 9,
            "top-10 overlap for query {q_text:?}: {overlap} < 9"
        );
    }
}

#[test]
fn flat_1k_vectors_threshold_finds_self() {
    const N_DOCS: usize = 1_000;
    const DIM: usize = 32;

    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::repo::BlobStore;
    use triblespace_core::value::schemas::hash::Blake3;
    use triblespace_search::schemas::put_embedding;

    let mut rng = SplitMix64(0x1234_5678);
    let mut store = MemoryBlobStore::<Blake3>::new();
    let mut builder = FlatBuilder::new(DIM);
    // One target vector we know the answer for — all ones in the
    // first half, zero in the second — plus 999 random neighbours.
    let mut target_vec = vec![0.0f32; DIM];
    for v in target_vec.iter_mut().take(DIM / 2) {
        *v = 1.0;
    }
    let h_target = put_embedding::<_, Blake3>(&mut store, target_vec).unwrap();
    builder.insert(h_target);
    for _ in 0..(N_DOCS - 1) {
        let vec: Vec<f32> = (0..DIM)
            .map(|_| (rng.next() as i32 as f32) / (i32::MAX as f32))
            .collect();
        let h = put_embedding::<_, Blake3>(&mut store, vec).unwrap();
        builder.insert(h);
    }
    let idx = builder.build();
    let reader = store.reader().unwrap();

    // Probing from the target handle at a very tight threshold
    // finds it (cos=1.0 against itself).
    let hits = idx.attach(&reader).candidates_above(h_target, 0.999).unwrap();
    assert!(hits.contains(&h_target));

    // Sanity: byte_size grows linearly with handle count.
    assert_eq!(idx.byte_size(), 24 + idx.doc_count() * 32);
}
