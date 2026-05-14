//! Vector search composed with a `pattern!` over a real
//! `TribleSet`, in a single `find!`.
//!
//! Scenario: a tiny catalog with books that have an author and
//! an embedding handle stored as a trible attribute. Build an
//! HNSW index over every embedding handle, put a query vector
//! into the same blob store, then ask the engine for "books
//! whose embedding is similar (cosine ≥ 0.8) to the query AND
//! that are authored by the target author". The HNSW
//! [`Similar`][s] constraint and the pattern! clause join
//! through the shared embedding-handle variable.
//!
//! ```sh
//! cargo run --example compose_hnsw_and_pattern
//! ```
//!
//! [s]: triblespace_search::constraint::Similar

use triblespace_core::and;
use triblespace_core::blob::MemoryBlobStore;
use triblespace_core::examples::literature;
use triblespace_core::find;
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::query::temp;
use triblespace_core::repo::BlobStore;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::value::Inline;
use triblespace_core::macros::{entity, pattern};
use triblespace_core::macros::attributes;

use triblespace_search::hnsw::HNSWBuilder;
use triblespace_search::schemas::{put_embedding, Embedding};

// Example-local attribute: `book → embedding handle`. The id was
// minted with `trible genid`; this crate doesn't commit to a
// standard attribute for embeddings because each caller picks
// the embedding schema they want.
mod search_attrs {
    use super::*;

    attributes! {
        "03712511F65DCC9B1C45FE04184F1B44" as pub book_embedding: Handle<Embedding>;
    }
}

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).expect("non-nil")
}

fn main() {
    // Authors + four books, two per author.
    let target_author = id(10);
    let other_author = id(11);
    let book_a = id(20); // target_author, near-query embedding
    let book_b = id(21); // target_author, far embedding
    let book_c = id(22); // other_author, near-query embedding
    let book_d = id(23); // other_author, far embedding

    // Put each book's embedding into the blob store up front so
    // we can reference them by handle from both the TribleSet
    // and the HNSW index — one source of truth for the vectors.
    let mut store = MemoryBlobStore::new();
    let vectors: Vec<(Id, Vec<f32>)> = vec![
        (book_a, vec![0.9, 0.1, 0.05, 0.02]),
        (book_b, vec![0.0, 0.0, 1.0, 0.0]),
        (book_c, vec![0.85, 0.15, 0.1, 0.0]),
        (book_d, vec![-1.0, 0.0, 0.0, 0.0]),
    ];
    let mut handles: std::collections::HashMap<Id, Inline<Handle<Embedding>>> =
        std::collections::HashMap::new();
    for (bid, v) in &vectors {
        let h = put_embedding::<_>(&mut store, v.clone()).unwrap();
        handles.insert(*bid, h);
    }

    // KB: authors + book metadata + each book's embedding handle.
    let mut kb = TribleSet::new();
    kb += entity! { ExclusiveId::force_ref(&target_author) @
        literature::firstname: "Target",
        literature::lastname: "Author",
    };
    kb += entity! { ExclusiveId::force_ref(&other_author) @
        literature::firstname: "Other",
        literature::lastname: "Author",
    };
    for (bid, title) in [
        (book_a, "A Near Tale"),
        (book_b, "A Distant Saga"),
        (book_c, "Close Encounters"),
        (book_d, "Unrelated Memoir"),
    ] {
        let author = if bid == book_a || bid == book_b {
            target_author
        } else {
            other_author
        };
        let h = handles[&bid];
        kb += entity! { ExclusiveId::force_ref(&bid) @
            literature::title: title,
            literature::author: &author,
            search_attrs::book_embedding: h,
        };
    }

    // HNSW index over every embedding handle.
    let mut hb = HNSWBuilder::new(4).with_seed(42);
    for (bid, v) in &vectors {
        hb.insert(handles[bid], v.clone()).unwrap();
    }
    let idx = hb.build();

    // Put the query vector into the store too — similarity is a
    // binary relation over handles, so the query lives in the
    // same address space as the corpus. Content-addressing makes
    // repeats free.
    let query_handle =
        put_embedding::<_>(&mut store, vec![1.0, 0.0, 0.0, 0.0]).unwrap();
    let reader = store.reader().unwrap();
    let view = idx.attach(&reader);
    println!(
        "HNSW index built: {} handles, dim = {}, max_level = {}",
        idx.doc_count(),
        idx.dim(),
        idx.max_level()
    );

    // Standalone similarity — should surface books A and C.
    // Even when there's no other constraint to compose with, the
    // engine path is the idiomatic shape: the same `similar_to`
    // call works whether you AND it with patterns or run it
    // alone. Reaching past the constraint into the leaf walk is
    // a benchmark/test pattern, not an application pattern.
    println!("\nsimilarity-only (no author filter), cos ≥ 0.8:");
    let similar_only: Vec<(Id,)> = find!(
        (book: Id),
        temp!(
            (emb),
            and!(
                view.similar_to(query_handle, emb, 0.8),
                pattern!(&kb, [{ ?book @
                    search_attrs::book_embedding: ?emb,
                }]),
            )
        )
    )
    .collect();
    for (b,) in &similar_only {
        println!("  {b}");
    }

    // Headline query: similar to the query AND authored by
    // target_author. The unary `similar_to` convenience pins
    // the probe handle on the call and binds `emb` to handles
    // clearing the cosine floor; the pattern joins them back
    // to book entities via the shared `emb` variable.
    println!("\nquery: similar to [1,0,0,0] AND author = target_author (cos ≥ 0.8)");
    let matches: Vec<(Id,)> = find!(
        (book: Id),
        temp!(
            (emb),
            and!(
                view.similar_to(query_handle, emb, 0.8),
                pattern!(&kb, [{ ?book @
                    literature::author: &target_author,
                    search_attrs::book_embedding: ?emb,
                }]),
            )
        )
    )
    .collect();
    println!("  {} rows:", matches.len());
    for (b,) in &matches {
        println!("    {b}");
    }

    // Expected: book_a (near query + right author). book_c is
    // near but wrong author; book_b is right author but below
    // the cosine floor.
    assert!(matches.iter().any(|(b,)| *b == book_a));
    assert!(!matches.iter().any(|(b,)| *b == book_c));
    assert!(!matches.iter().any(|(b,)| *b == book_b));
}
