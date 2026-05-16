//! End-to-end smoke test: does a succinct index actually
//! survive a real `BlobStore::put` / `BlobStoreGet::get` cycle?
//!
//! The in-crate BlobEncoding tests exercise `to_blob` /
//! `try_from_blob` directly. This test closes the last-mile
//! loop: go through the triblespace repo traits so we know the
//! handle-typed API works unmodified with SB25 + SH25 blobs.
//!
//! Uses `MemoryBlobStore` rather than an on-disk pile because
//! the test is about the API chain, not file I/O — the
//! pile-backed write path exercises the same traits.

use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::find;
use triblespace_core::id::Id;
use triblespace_core::repo::{BlobStoreGet, BlobStorePut};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;

use triblespace_search::bm25::BM25Builder;
use triblespace_search::hnsw::HNSWBuilder;
use triblespace_search::schemas::Embedding;
use triblespace_search::succinct::{
    SuccinctBM25Blob, SuccinctBM25Index, SuccinctHNSWBlob, SuccinctHNSWIndex,
};
use triblespace_search::tokens::hash_tokens;

fn iid(byte: u8) -> Id {
    Id::new([byte; 16]).unwrap()
}

#[test]
fn succinct_bm25_survives_blob_store_roundtrip() {
    // Build a small index.
    let mut b: BM25Builder = BM25Builder::new();
    b.insert(iid(1), hash_tokens("the quick brown fox"));
    b.insert(iid(2), hash_tokens("the lazy brown dog"));
    b.insert(iid(3), hash_tokens("quick silver fox jumps"));
    let original = b.build();

    // Put → handle.
    let mut store = MemoryBlobStore::new();
    let handle = store
        .put::<SuccinctBM25Blob, _>(&original)
        .expect("put should succeed");

    // Get → reloaded view.
    let reader = <MemoryBlobStore as triblespace_core::repo::BlobStore>::reader(&mut store)
        .expect("reader");
    let reloaded: SuccinctBM25Index = reader
        .get::<SuccinctBM25Index, SuccinctBM25Blob>(handle)
        .expect("get should succeed");

    // Same corpus descriptors.
    assert_eq!(reloaded.doc_count(), original.doc_count());
    assert_eq!(reloaded.term_count(), original.term_count());
    assert_eq!(reloaded.k1(), original.k1());
    assert_eq!(reloaded.b(), original.b());
    assert!((reloaded.avg_doc_len() - original.avg_doc_len()).abs() < 1e-6);

    // Same query answer for "fox".
    let fox = hash_tokens("fox")[0];
    let a: Vec<_> = original.query_term(&fox).collect();
    let r: Vec<_> = reloaded.query_term(&fox).collect();
    assert_eq!(a.len(), r.len());
    let tol = reloaded.score_tolerance().max(1e-5);
    for ((a_id, a_s), (r_id, r_s)) in a.iter().zip(r.iter()) {
        assert_eq!(a_id, r_id);
        assert!(
            (a_s - r_s).abs() <= tol,
            "score drift after pile round-trip: {a_s} vs {r_s} > tol {tol}"
        );
    }
}

#[test]
fn succinct_hnsw_survives_blob_store_roundtrip() {
    use std::collections::HashSet;
    
    use triblespace_search::schemas::put_embedding;

    // Build a small HNSW index.
    let mut store = MemoryBlobStore::new();
    let mut b = HNSWBuilder::new(4).with_seed(9);
    let mut handles = Vec::new();
    for i in 1..=12u8 {
        let f = i as f32;
        let v = vec![f.sin(), f.cos(), (f * 0.5).sin(), (f * 0.3).cos()];
        let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
        b.insert(h, v).unwrap();
        handles.push(h);
    }
    let original = b.build();

    // Put the index itself as a blob alongside the embedding
    // blobs it references.
    let handle = store
        .put::<SuccinctHNSWBlob, _>(&original)
        .expect("put should succeed");

    // Get → reloaded view, then attach the reader for queries.
    let reader = <MemoryBlobStore as triblespace_core::repo::BlobStore>::reader(&mut store)
        .expect("reader");
    let reloaded: SuccinctHNSWIndex = reader
        .get::<SuccinctHNSWIndex, SuccinctHNSWBlob>(handle)
        .expect("get should succeed");

    assert_eq!(reloaded.doc_count(), original.doc_count());
    assert_eq!(reloaded.dim(), original.dim());
    assert_eq!(reloaded.max_level(), original.max_level());

    // Same above-threshold set for the same probe handle. Going
    // through the engine path (`find!` + `similar_to`) here
    // rather than the leaf `candidates_above`: that's the
    // pipeline a real consumer runs after loading an index from
    // a pile, and it's the shape we want tests to demonstrate.
    let probe = handles[0];
    let original_view = original.attach(&reader);
    let reloaded_view = reloaded.attach(&reader);
    let a: HashSet<Inline<Handle<Embedding>>> = find!(
        (n: Inline<Handle<Embedding>>),
        original_view.similar_to(probe, n, 0.4)
    )
    .map(|(h,)| h)
    .collect();
    let r: HashSet<Inline<Handle<Embedding>>> = find!(
        (n: Inline<Handle<Embedding>>),
        reloaded_view.similar_to(probe, n, 0.4)
    )
    .map(|(h,)| h)
    .collect();
    assert_eq!(a, r, "pile round-trip diverged on {probe:?}");
}

/// The `Embedding` blob schema is content-addressed, so two
/// HNSW indexes that embed the same vectors end up with the same
/// handles — and share the same underlying blobs in the store.
/// Explicitly test that load-bearing property.
#[test]
fn hnsw_indexes_share_embedding_blobs() {
    use triblespace_core::repo::BlobStore;
    
    use triblespace_search::schemas::put_embedding;

    let vecs = [
        vec![1.0f32, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 0.0],
    ];

    // Build two independent HNSW indexes over the same
    // embeddings. Each index owns its own graph state (M / M0,
    // seed, neighbour lists), but the stored handles must be
    // identical — content-addressing is deterministic on
    // normalized bytes.
    let mut store = MemoryBlobStore::new();

    let mut a_b = HNSWBuilder::new(4).with_seed(1);
    for v in &vecs {
        let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
        a_b.insert(h, v.clone()).unwrap();
    }
    // `build_naive()` so the test can inspect `handles()[i]`
    // directly — the succinct form wraps handles in a
    // `FixedBytesTable`.
    let idx_a = a_b.build_naive();

    let mut b_b = HNSWBuilder::new(4).with_seed(99); // different seed!
    for v in &vecs {
        let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
        b_b.insert(h, v.clone()).unwrap();
    }
    let idx_b = b_b.build_naive();

    // Handles must be identical in the same slot order.
    assert_eq!(idx_a.doc_count(), idx_b.doc_count());
    for i in 0..idx_a.doc_count() {
        assert_eq!(
            idx_a.handles()[i],
            idx_b.handles()[i],
            "handle mismatch at i={i} — content-address dedup failed"
        );
    }

    // Store size check: we did 6 `put_embedding` calls but
    // only 3 distinct vectors were supplied, so only 3 blobs
    // survive in the reader's view.
    let reader = store.reader().unwrap();
    assert_eq!(
        reader.len(),
        3,
        "expected 3 unique embedding blobs, found {}",
        reader.len()
    );

    // Both indexes, attached to the same reader, find the shared
    // handle when probed from it — the exact match is always
    // above any finite threshold. Engine path through `find!`,
    // matching what a consumer actually runs.
    let probe = idx_a.handles()[0];
    let view_a = idx_a.attach(&reader);
    let view_b = idx_b.attach(&reader);
    let hits_a: Vec<_> = find!(
        (n: Inline<Handle<Embedding>>),
        view_a.similar_to(probe, n, 0.99)
    )
    .map(|(h,)| h)
    .collect();
    let hits_b: Vec<_> = find!(
        (n: Inline<Handle<Embedding>>),
        view_b.similar_to(probe, n, 0.99)
    )
    .map(|(h,)| h)
    .collect();
    assert!(hits_a.contains(&probe));
    assert!(hits_b.contains(&probe));
}
