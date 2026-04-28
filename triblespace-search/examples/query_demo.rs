//! Minimal end-to-end demo: build a BM25 index over a handful of
//! doc strings, serialize it to bytes, reload, query.
//!
//! ```sh
//! cargo run --example query_demo
//! ```

use triblespace_core::id::Id;
use triblespace_core::macros::find;
use triblespace_core::value::schemas::genid::GenId;
use triblespace_core::value::{ToValue, Value};
use triblespace_search::bm25::BM25Builder;
use triblespace_search::succinct::SuccinctBM25Index;
use triblespace_search::tokens::hash_tokens;

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).expect("non-nil")
}

fn main() {
    // A corpus of five fragments.
    let corpus = [
        (id(1), "Typst is a markup-based typesetting system."),
        (
            id(2),
            "Wiki fragments are small typst documents linked by id.",
        ),
        (id(3), "BM25 scores documents by term frequency and IDF."),
        (
            id(4),
            "Each fragment cites other fragments via wiki: links.",
        ),
        (id(5), "Cosine similarity ranks embeddings by direction."),
    ];

    // Build.
    let mut builder = BM25Builder::new();
    for (id, text) in &corpus {
        builder.insert(*id, hash_tokens(text));
        println!("indexed {id} ({} tokens)", hash_tokens(text).len());
    }
    let idx = builder.build();
    println!(
        "\nindex: {} docs, {} terms, avg_doc_len = {:.2}",
        idx.doc_count(),
        idx.term_count(),
        idx.avg_doc_len()
    );

    // Serialize round-trip — the same bytes end-to-end. With
    // canonical-bytes the index *is* its blob, so the round trip
    // is a refcounted handover (`to_blob` + `try_from_blob`).
    use triblespace_core::blob::{ToBlob, TryFromBlob};
    let blob = (&idx).to_blob();
    let reloaded: SuccinctBM25Index =
        SuccinctBM25Index::try_from_blob(blob.clone()).expect("valid blob");
    println!("\nblob size: {} bytes", blob.bytes.len());
    assert_eq!(reloaded.doc_count(), idx.doc_count());

    // Single-term query — iterate the posting list directly.
    println!("\nquery: 'typst'");
    let q = hash_tokens("typst");
    for (doc, score) in reloaded.query_term(&q[0]) {
        let id: Id = doc.try_from_value().expect("genid posting");
        println!("  {id}  score={score:.3}");
    }

    // Multi-term ranking: filter via `matches_text` (tokenises
    // the query string internally — no `hash_tokens` ceremony),
    // score precisely via `score_text`, sort, truncate.
    println!("\nquery: 'fragment wiki'");
    let mut hits: Vec<(Id, f32)> = find!(
        (doc: Id),
        reloaded.matches_text(doc, "fragment wiki", 0.0)
    )
    .map(|(d,)| (d, reloaded.score_text(&(&d).to_value(), "fragment wiki")))
    .collect();
    hits.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    for (doc, score) in hits.into_iter().take(3) {
        println!("  {doc}  score={score:.3}");
    }

    // Value-as-term: use a doc's Id as a "citation term" and
    // index a new micro-corpus where each doc is a list of the
    // fragments it cites. The same BM25 index gives us
    // "documents citing this fragment".
    println!("\ncitation search (term = fragment id):");
    // When terms themselves are entity ids, both `D` and `T`
    // are `GenId` — the same BM25 index handles "docs containing
    // a mention-of-entity-X term."
    let mut cite_builder: BM25Builder<GenId, GenId> = BM25Builder::new();
    cite_builder.insert(id(10), vec![(&id(1)).to_value()]);
    cite_builder.insert(id(11), vec![(&id(1)).to_value(), (&id(3)).to_value()]);
    cite_builder.insert(id(12), vec![(&id(3)).to_value()]);
    let cite_idx = cite_builder.build();

    let citation_term: Value<GenId> = (&id(1)).to_value();
    let cites_one: Vec<(Id, f32)> = cite_idx
        .query_term(&citation_term)
        .filter_map(|(v, s)| v.try_from_value().ok().map(|id: Id| (id, s)))
        .collect();
    println!("  citations of {}: {} doc(s)", id(1), cites_one.len());
    for (doc, _) in cites_one {
        println!("    cited by {doc}");
    }
}
