//! On-disk triblespace::Pile round-trip for both succinct index
//! types.
//!
//! Complements `tests/pile_roundtrip.rs` (which uses
//! `MemoryBlobStore`) by exercising the actual mmap-backed Pile
//! — catches any serialization bugs that only surface when the
//! blob lands on disk and is read back through the memory
//! mapping, and confirms the end-to-end "write, close, reopen,
//! query" flow a real faculty would use.

use tempfile::tempdir;
use triblespace_core::find;
use triblespace_core::id::Id;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut};
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Value;

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
fn succinct_bm25_survives_pile_round_trip() {
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("bm25.pile");
    // Pile::open wants the file to exist; create it empty.
    std::fs::File::create(&pile_path).expect("create pile file");

    // Build and persist.
    let mut b = BM25Builder::new();
    b.insert(iid(1), hash_tokens("the quick brown fox"));
    b.insert(iid(2), hash_tokens("the lazy brown dog"));
    b.insert(iid(3), hash_tokens("quick silver fox jumps"));
    let original = b.build();

    let handle = {
        let mut pile = Pile::<triblespace_core::value::schemas::hash::Blake3>::open(&pile_path)
            .expect("open pile");
        pile.refresh().expect("refresh empty pile");
        let h = pile
            .put::<SuccinctBM25Blob, _>(&original)
            .expect("put SB25");
        pile.flush().expect("flush");
        h
    };

    // Reopen — exercises the actual on-disk load path.
    let mut pile = Pile::<triblespace_core::value::schemas::hash::Blake3>::open(&pile_path)
        .expect("reopen pile");
    pile.refresh().expect("refresh");
    let reader = pile.reader().expect("reader");
    let reloaded: SuccinctBM25Index = reader
        .get::<SuccinctBM25Index, SuccinctBM25Blob>(handle)
        .expect("get");

    assert_eq!(reloaded.doc_count(), original.doc_count());
    assert_eq!(reloaded.term_count(), original.term_count());

    // Same query must return the same postings.
    let fox = hash_tokens("fox")[0];
    let a: Vec<_> = original.query_term(&fox).collect();
    let r: Vec<_> = reloaded.query_term(&fox).collect();
    assert_eq!(a.len(), r.len());
    let tol = reloaded.score_tolerance().max(1e-5);
    for ((a_id, a_s), (r_id, r_s)) in a.iter().zip(r.iter()) {
        assert_eq!(a_id, r_id);
        assert!((a_s - r_s).abs() <= tol);
    }
}

#[test]
fn succinct_hnsw_survives_pile_round_trip() {
    use std::collections::HashSet;
    use triblespace_core::value::schemas::hash::Blake3;
    use triblespace_search::schemas::put_embedding;

    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("hnsw.pile");
    std::fs::File::create(&pile_path).expect("create pile file");

    let (original, probe, handle) = {
        let mut pile = Pile::<Blake3>::open(&pile_path).expect("open pile");
        pile.refresh().expect("refresh empty pile");

        let mut b = HNSWBuilder::new(4).with_seed(19);
        let mut handles = Vec::new();
        for i in 1..=12u8 {
            let f = i as f32;
            let v = vec![f.sin(), f.cos(), (f * 0.5).sin(), (f * 0.3).cos()];
            let h = put_embedding::<_, Blake3>(&mut pile, v.clone()).unwrap();
            b.insert(h, v).unwrap();
            handles.push(h);
        }
        let original = b.build();

        let handle = pile
            .put::<SuccinctHNSWBlob, _>(&original)
            .expect("put SH25");
        pile.flush().expect("flush");
        (original, handles[0], handle)
    };

    let mut pile = Pile::<Blake3>::open(&pile_path).expect("reopen pile");
    pile.refresh().expect("refresh");
    let reader = pile.reader().expect("reader");
    let reloaded: SuccinctHNSWIndex = reader
        .get::<SuccinctHNSWIndex, SuccinctHNSWBlob>(handle)
        .expect("get");

    assert_eq!(reloaded.doc_count(), original.doc_count());
    assert_eq!(reloaded.dim(), original.dim());

    // Engine path: same shape a real faculty runs after
    // reopening a pile and getting an index handle. Tests the
    // full pipeline (constraint construction → engine eval →
    // result enumeration) survives the on-disk round-trip, not
    // just the leaf walk.
    let original_view = original.attach(&reader);
    let reloaded_view = reloaded.attach(&reader);
    let a: HashSet<Value<Handle<Blake3, Embedding>>> = find!(
        (n: Value<Handle<Blake3, Embedding>>),
        original_view.similar_to(probe, n, 0.4)
    )
    .map(|(h,)| h)
    .collect();
    let r: HashSet<Value<Handle<Blake3, Embedding>>> = find!(
        (n: Value<Handle<Blake3, Embedding>>),
        reloaded_view.similar_to(probe, n, 0.4)
    )
    .map(|(h,)| h)
    .collect();
    assert_eq!(a, r, "on-disk round-trip diverged");
}
