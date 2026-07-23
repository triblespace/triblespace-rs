//! Phrase-aware BM25 retrieval via two typed indexes.
//!
//! Words and bigrams live in different term schemas
//! ([`WordHash`] vs [`BigramHash`]), so they can't share an
//! index — the compiler enforces the separation. The pattern is
//! two [`BM25Builder`]s, one per tokenizer, keyed by the same
//! doc id; hybrid queries combine both via the query engine.
//!
//! ```sh
//! cargo run --example phrase_search
//! ```

use std::collections::HashMap;

use triblespace_core::id::Id;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{Inline, TryFromInline};

use triblespace_search::bm25::BM25Builder;
use triblespace_search::succinct::SuccinctBM25Index;
use triblespace_search::tokens::{bigram_tokens, hash_tokens, BigramHash, WordHash};

/// Decode a `Inline<GenId>` posting key to its underlying `Id`.
/// Helper used to recover typed `Id`s from posting iterators
/// after the `*_ids` shortcuts were dropped.
fn id_from_value(v: Inline<GenId>) -> Id {
    Id::try_from_inline(&v).expect("genid posting")
}

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).expect("non-nil")
}

fn main() {
    // A corpus where "fox" appears in all four docs, but only
    // one has "quick brown" adjacent and two have "brown fox"
    // adjacent.
    let corpus = [
        (id(1), "the quick brown fox jumps"),
        (id(2), "a quick silver fox"),
        (id(3), "the brown fox runs fast"),
        (id(4), "quick fox and brown dog"),
    ];

    // ── Two typed indexes, same doc keys, different term
    // schemas. The compiler will refuse to cross-query them. ─
    let mut words_b: BM25Builder<GenId, WordHash> = BM25Builder::new();
    let mut bigrams_b: BM25Builder<GenId, BigramHash> = BM25Builder::new();
    for (doc_id, text) in &corpus {
        words_b.insert(doc_id, hash_tokens(text));
        bigrams_b.insert(doc_id, bigram_tokens(text));
    }
    let words: SuccinctBM25Index<GenId, WordHash> = words_b.build();
    let bigrams: SuccinctBM25Index<GenId, BigramHash> = bigrams_b.build();
    println!(
        "indexes: {} docs · {} words · {} bigrams\n",
        words.doc_count(),
        words.term_count(),
        bigrams.term_count(),
    );

    // ── 1. Single-word query: "fox" hits every doc. Runs on the
    // words index. `bigrams.query_term(&hash_tokens("fox")[0])`
    // wouldn't compile — wrong term schema. ─
    let word_fox = &hash_tokens("fox")[0];
    let hits: Vec<(Id, f32)> = words
        .query_term(word_fox)
        .map(|(v, s)| (id_from_value(v), s))
        .collect();
    println!("single-word query 'fox':");
    for (d, s) in &hits {
        let text = text_for(&corpus, d);
        println!("  {d}  score={s:.3}  {text}");
    }
    assert_eq!(hits.len(), 4);

    // ── 2. Phrase query: "quick brown" — one bigram. Runs on
    // the bigrams index; only docs with that ordered pair
    // adjacently match. ─
    let phrase = &bigram_tokens("quick brown")[0];
    let hits: Vec<(Id, f32)> = bigrams
        .query_term(phrase)
        .map(|(v, s)| (id_from_value(v), s))
        .collect();
    println!("\nphrase query 'quick brown' (adjacent only):");
    for (d, s) in &hits {
        let text = text_for(&corpus, d);
        println!("  {d}  score={s:.3}  {text}");
    }
    // doc 1: "...quick brown fox..." ✓
    // doc 4: "quick fox and brown dog" — not adjacent, skipped.
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, id(1));

    // ── 3. Phrase query: "brown fox" — matches docs 1 and 3. ─
    let phrase = &bigram_tokens("brown fox")[0];
    let hits: Vec<(Id, f32)> = bigrams
        .query_term(phrase)
        .map(|(v, s)| (id_from_value(v), s))
        .collect();
    println!("\nphrase query 'brown fox':");
    for (d, s) in &hits {
        let text = text_for(&corpus, d);
        println!("  {d}  score={s:.3}  {text}");
    }
    assert_eq!(hits.len(), 2);
    let ids: Vec<_> = hits.iter().map(|(d, _)| *d).collect();
    assert!(ids.contains(&id(1)));
    assert!(ids.contains(&id(3)));

    // ── 4. Hybrid: combine word + bigram evidence for a longer
    // phrase. Each index contributes its own BM25 scores; we
    // sum across the two to rank docs. In an engine-level
    // composition you'd `or!` the two constraints and get the
    // same shape for free. ─
    println!("\nhybrid query 'the quick brown' (words ∪ bigrams):");
    let word_terms = hash_tokens("the quick brown");
    let bigram_terms = bigram_tokens("the quick brown");
    let mut acc: HashMap<Id, f32> = HashMap::new();
    for t in &word_terms {
        for (v, s) in words.query_term(t) {
            *acc.entry(id_from_value(v)).or_default() += s;
        }
    }
    for t in &bigram_terms {
        for (v, s) in bigrams.query_term(t) {
            *acc.entry(id_from_value(v)).or_default() += s;
        }
    }
    let mut ranked: Vec<_> = acc.into_iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    for (d, s) in ranked.iter().take(3) {
        let text = text_for(&corpus, d);
        println!("  {d}  score={s:.3}  {text}");
    }
    assert_eq!(
        ranked[0].0,
        id(1),
        "doc 1 has both 'the quick' AND 'quick brown' bigrams + all three words",
    );
}

fn text_for<'a>(corpus: &'a [(Id, &'a str)], d: &Id) -> &'a str {
    corpus
        .iter()
        .find(|(i, _)| i == d)
        .map(|(_, t)| *t)
        .unwrap_or("?")
}
