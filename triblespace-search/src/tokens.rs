//! Opt-in helpers for turning strings into the 32-byte
//! triblespace `Value`s that `bm25::BM25Index` uses as term ids.
//!
//! Nothing in `bm25::` or `hnsw::` depends on this module —
//! callers who have their own tokenizer (language-specific
//! stemming, typst/code-aware splitting, phrase handling) can
//! feed `Value`s directly and skip these helpers entirely.
//!
//! # Per-tokenizer schemas
//!
//! Each tokenizer produces a distinct [`ValueSchema`], so the
//! compiler keeps their outputs from mixing at the type level
//! (the old string-prefix approach was a statistical safeguard
//! only — user text containing literal `"3:foo"` could collide
//! with `ngram_tokens("foo", 3)`):
//!
//! - [`hash_tokens`] + [`code_tokens`] → [`Value<WordHash>`]
//!   (same output space — both lowercase + Blake3).
//! - [`bigram_tokens`] → [`Value<BigramHash>`].
//! - [`ngram_tokens`] → [`Value<NgramHash>`].
//!
//! An index that needs multiple tokenizer flavors becomes
//! multiple indexes, one per schema, joined via `and!` / `or!`
//! at query time — see `examples/phrase_search.rs`.

use std::convert::Infallible;

use triblespace_core::id::Id;
use triblespace_core::id_hex;
use triblespace_core::macros::entity;
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::trible::{Fragment, TribleSet};
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::{Value, ValueSchema};

/// Term schema for [`hash_tokens`] and [`code_tokens`] — both
/// produce Blake3 hashes of a lowercased word / code segment.
///
/// Schema id minted via `trible genid`:
/// `8868FA39C4CDA947DD4CAA1652C30D06`.
pub enum WordHash {}

impl MetaDescribe for WordHash {
    fn describe() -> Fragment {
        let mut fragment = Fragment::rooted(
            id_hex!("8868FA39C4CDA947DD4CAA1652C30D06"),
            TribleSet::new(),
        );
        let name = fragment.put("WordHash");
        let description = fragment.put(
            "Term schema for hash_tokens / code_tokens — Blake3 hash of a lowercased word or code segment.",
        );
        fragment.annotated(|id_ref| {
            entity! { id_ref @
                metadata::name:        name,
                metadata::description: description,
                metadata::tag:         metadata::KIND_VALUE_SCHEMA,
            }
        })
    }
}

impl ValueSchema for WordHash {
    type ValidationError = Infallible;
}

/// Term schema for [`bigram_tokens`] — Blake3 hash of a pair of
/// adjacent lowercased words, NUL-delimited.
///
/// Schema id minted via `trible genid`:
/// `2EC1CAAD948B959D32023EF32D500148`.
pub enum BigramHash {}

impl MetaDescribe for BigramHash {
    fn describe() -> Fragment {
        let mut fragment = Fragment::rooted(
            id_hex!("2EC1CAAD948B959D32023EF32D500148"),
            TribleSet::new(),
        );
        let name = fragment.put("BigramHash");
        let description = fragment.put(
            "Term schema for bigram_tokens — Blake3 hash of a pair of adjacent lowercased words, NUL-delimited.",
        );
        fragment.annotated(|id_ref| {
            entity! { id_ref @
                metadata::name:        name,
                metadata::description: description,
                metadata::tag:         metadata::KIND_VALUE_SCHEMA,
            }
        })
    }
}

impl ValueSchema for BigramHash {
    type ValidationError = Infallible;
}

/// Term schema for [`ngram_tokens`] — Blake3 hash of a
/// character n-gram window, with the n-size prefixed into the
/// hash input so different `n` values don't collide within the
/// same schema.
///
/// Schema id minted via `trible genid`:
/// `52472B53D201532D7FAA7D89AE80A6ED`.
pub enum NgramHash {}

impl MetaDescribe for NgramHash {
    fn describe() -> Fragment {
        let mut fragment = Fragment::rooted(
            id_hex!("52472B53D201532D7FAA7D89AE80A6ED"),
            TribleSet::new(),
        );
        let name = fragment.put("NgramHash");
        let description = fragment.put(
            "Term schema for ngram_tokens — Blake3 hash of a character n-gram window, with the n-size prefixed into the hash input so different n values don't collide within the same schema.",
        );
        fragment.annotated(|id_ref| {
            entity! { id_ref @
                metadata::name:        name,
                metadata::description: description,
                metadata::tag:         metadata::KIND_VALUE_SCHEMA,
            }
        })
    }
}

impl ValueSchema for NgramHash {
    type ValidationError = Infallible;
}

/// Tokenize `text` with a simple whitespace-and-lowercase scheme
/// and return each token as a 32-byte Blake3 hash suitable for
/// use as a `bm25::BM25Index` term value.
///
/// Rules:
/// - Split on ASCII whitespace (`char::is_ascii_whitespace`).
/// - Trim leading/trailing ASCII punctuation from each token.
/// - Lowercase ASCII letters; leave non-ASCII bytes as-is.
/// - Drop empty tokens (after trimming).
/// - Duplicates are preserved — the index uses term frequency.
///
/// The hashing is fixed (Blake3) so the same token produces the
/// same 32-byte value across processes and crate versions. That
/// matters because a `bm25::SuccinctBM25Index` stores these
/// hashes directly; callers who want language-aware tokenization
/// should write their own `&str -> Vec<RawValue>` function and
/// skip this helper.
///
/// # Example
///
/// ```
/// # use triblespace_search::tokens::hash_tokens;
/// let vs = hash_tokens("Hello, WORLD — hello.");
/// // "hello" appears twice with the same hash; "world" once.
/// assert_eq!(vs.len(), 3);
/// assert_eq!(vs[0], vs[2]);
/// assert_ne!(vs[0], vs[1]);
/// ```
pub fn hash_tokens(text: &str) -> Vec<Value<WordHash>> {
    normalize_words(text)
        .map(|w| Value::<WordHash>::new(*blake3::hash(w.as_bytes()).as_bytes()))
        .collect()
}

/// Shared word-normalization pipeline used by [`hash_tokens`]
/// and [`bigram_tokens`]: split on ASCII whitespace, trim
/// leading/trailing ASCII punctuation, drop tokens with no
/// alphanumeric content, lowercase ASCII letters. Returns
/// owned `String`s since callers typically hash them (and
/// bigram_tokens pairs them) — a &str view would borrow the
/// source text, which we can't express across the
/// per-character lowercase step without allocating anyway.
fn normalize_words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split_ascii_whitespace().filter_map(|raw| {
        let trimmed = raw.trim_matches(|c: char| c.is_ascii_punctuation());
        // Drop tokens with no alphanumeric content at all
        // (em-dashes, pure-symbol clusters). Pure-punctuation
        // tokens would otherwise all hash to the same value
        // and poison the term list.
        if !trimmed.chars().any(|c| c.is_alphanumeric()) {
            return None;
        }
        let mut lower = String::with_capacity(trimmed.len());
        for c in trimmed.chars() {
            lower.push(c.to_ascii_lowercase());
        }
        Some(lower)
    })
}

/// Word-level bigram tokenizer for phrase-aware retrieval.
///
/// Tokenizes `text` with the same rules as [`hash_tokens`]
/// (whitespace split + lowercase + punctuation trim + drop
/// empty), then emits one hashed value per *adjacent pair* of
/// resulting tokens. Each bigram is namespaced with a `"2w:"`
/// prefix before hashing so it lives in its own term-space
/// separate from single-word hashes.
///
/// Concatenating `hash_tokens(text)` with `bigram_tokens(text)`
/// before `BM25Builder::insert` lets the same BM25 index answer
/// both single-word queries (via the hash_tokens half) and
/// phrase queries (`bigram_tokens("quick brown")` produces the
/// `(quick, brown)` bigram hash, which only matches docs that
/// contain those two words adjacently).
///
/// Rules:
/// - Fewer than 2 tokens → empty output.
/// - Duplicates are preserved; running bigrams through the same
///   text produces the same values across processes and crate
///   versions.
///
/// # Example
///
/// ```
/// # use triblespace_search::tokens::bigram_tokens;
/// let grams = bigram_tokens("The quick brown fox");
/// // 4 words → 3 bigrams: (the, quick), (quick, brown),
/// // (brown, fox).
/// assert_eq!(grams.len(), 3);
///
/// // Phrase match: the query shares one bigram with the doc.
/// let doc = bigram_tokens("a quick brown dog");
/// let qry = bigram_tokens("quick brown");
/// assert!(doc.contains(&qry[0]));
/// ```
pub fn bigram_tokens(text: &str) -> Vec<Value<BigramHash>> {
    // Same normalization pipeline as `hash_tokens`, but we pair
    // adjacent words before hashing rather than hashing each on
    // its own.
    let words: Vec<String> = normalize_words(text).collect();
    if words.len() < 2 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(words.len() - 1);
    for pair in words.windows(2) {
        // NUL-delimited pair so the same glyph sequence with
        // different word boundaries can't collide (e.g. "ab c"
        // vs "a bc"). Cross-tokenizer separation now lives in
        // the [`BigramHash`] schema rather than a byte prefix.
        let mut buf = String::with_capacity(pair[0].len() + pair[1].len() + 1);
        buf.push_str(&pair[0]);
        buf.push('\u{0}');
        buf.push_str(&pair[1]);
        out.push(Value::<BigramHash>::new(*blake3::hash(buf.as_bytes()).as_bytes()));
    }
    out
}

/// Tokenizer for source-code-like identifiers. Splits on:
/// - any non-alphanumeric character (treating underscore,
///   hyphen, whitespace, punctuation as boundaries), and
/// - camelCase / acronym transitions inside a word:
///   - lowercase → uppercase boundary (`parseHTML` → `parse`,
///     `HTML`)
///   - uppercase-run → mixed-case boundary (`HTMLParser` →
///     `HTML`, `Parser`)
///   - letter → digit boundary (`parseV2` → `parse`, `V2`)
///   - digit → letter boundary (`2nd` → `2`, `nd`)
///
/// Each resulting segment is lowercased and Blake3-hashed, giving
/// a 32-byte term value compatible with [`hash_tokens`].
///
/// # Example
///
/// ```
/// # use triblespace_search::tokens::code_tokens;
/// // camelCase + acronym + snake_case all combine cleanly.
/// // `parseHTMLResponse_v2` → parse, html, response, v, 2.
/// let t = code_tokens("parseHTMLResponse_v2");
/// assert_eq!(t.len(), 5);
/// ```
///
/// Single-letter lowercase runs directly following an all-caps
/// acronym (e.g. `HTMLv` in `HTMLv2`) are ambiguous; the
/// tokenizer follows the standard convention of keeping the last
/// uppercase with the new lowercase run, producing `HTM` + `Lv`
/// in that case. Prefer explicit separators (`_`, case changes,
/// or digits) when the intent matters.
pub fn code_tokens(text: &str) -> Vec<Value<WordHash>> {
    let mut segments: Vec<String> = Vec::new();
    let mut cur = String::new();

    #[derive(Clone, Copy, PartialEq)]
    enum Kind {
        Lower,
        Upper,
        Digit,
        None,
    }
    fn kind(c: char) -> Kind {
        if c.is_ascii_digit() {
            Kind::Digit
        } else if c.is_uppercase() {
            Kind::Upper
        } else if c.is_lowercase() {
            Kind::Lower
        } else {
            Kind::None
        }
    }

    let chars: Vec<char> = text.chars().collect();
    let mut prev = Kind::None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let k = kind(c);

        // Non-alphanumeric: boundary. Flush and skip.
        if !c.is_alphanumeric() {
            if !cur.is_empty() {
                segments.push(std::mem::take(&mut cur));
            }
            prev = Kind::None;
            i += 1;
            continue;
        }

        // Case transitions:
        let split_here = match (prev, k) {
            (Kind::Lower, Kind::Upper) => true,
            (Kind::Lower, Kind::Digit) => true,
            (Kind::Digit, Kind::Lower) => true,
            (Kind::Digit, Kind::Upper) => true,
            (Kind::Upper, Kind::Digit) => true,
            // Uppercase-run → mixed-case boundary:
            // `HTMLParser` — when we see `P` after `L`, nothing;
            // but `r` after `P` starts a new lowercase run while
            // `HTMLP` has already been accumulated. The
            // standard rule: on Upper→Lower transition, if the
            // accumulated run has ≥2 uppercase letters, pop the
            // last upper off into the new segment.
            (Kind::Upper, Kind::Lower) if cur.chars().count() >= 2 => {
                // Move the last char of `cur` into a fresh
                // segment before `c`.
                let popped = cur.pop().unwrap();
                segments.push(std::mem::take(&mut cur));
                cur.push(popped);
                false
            }
            _ => false,
        };
        if split_here && !cur.is_empty() {
            segments.push(std::mem::take(&mut cur));
        }
        cur.push(c);
        prev = k;
        i += 1;
    }
    if !cur.is_empty() {
        segments.push(cur);
    }

    segments
        .into_iter()
        .filter_map(|s| {
            // Already lowercased-ready? Normalize to lowercase.
            let lower: String = s.chars().map(|c| c.to_ascii_lowercase()).collect();
            if lower.is_empty() {
                None
            } else {
                Some(Value::<WordHash>::new(*blake3::hash(lower.as_bytes()).as_bytes()))
            }
        })
        .collect()
}

/// Character-level n-gram tokenizer. Returns one hashed term per
/// sliding window of `n` characters across the lowercased text.
///
/// Indexing the same document with *both* `hash_tokens` and
/// `ngram_tokens(text, 3)` (concatenating the two `Vec`s before
/// `BM25Builder::insert`) lets the same BM25 index serve:
/// - whole-word queries via `hash_tokens("foo")`, and
/// - prefix / typo queries via `ngram_tokens("fox", 3)` — any
///   shared 3-gram boosts the score even when the surface forms
///   differ.
///
/// Rules:
/// - Non-alphanumeric characters are replaced by a single space
///   (so n-grams never cross punctuation/whitespace boundaries).
/// - Letters are lowercased.
/// - Runs shorter than `n` characters are dropped — no padding.
/// - `n == 0` returns an empty `Vec`.
///
/// Each n-gram is namespaced to `n` before hashing, so a trigram
/// `"fox"` and a bigram `"fo"` + `"ox"` produce distinct term
/// values and can coexist in one index.
///
/// # Example
///
/// ```
/// # use triblespace_search::tokens::ngram_tokens;
/// let tris = ngram_tokens("fox", 3);
/// assert_eq!(tris.len(), 1); // just "fox"
///
/// let tris = ngram_tokens("foxes", 3);
/// // "fox", "oxe", "xes"
/// assert_eq!(tris.len(), 3);
///
/// // "fox" and "foxes" share the "fox" trigram.
/// assert!(ngram_tokens("foxes", 3).contains(&ngram_tokens("fox", 3)[0]));
/// ```
pub fn ngram_tokens(text: &str, n: usize) -> Vec<Value<NgramHash>> {
    if n == 0 {
        return Vec::new();
    }

    // Normalize: lowercase letters, replace other non-alphanumerics
    // with a single space so runs don't merge across boundaries.
    let mut normalized = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_alphanumeric() {
            for l in c.to_lowercase() {
                normalized.push(l);
            }
        } else {
            normalized.push(' ');
        }
    }

    let mut out = Vec::new();
    for run in normalized.split_ascii_whitespace() {
        let chars: Vec<char> = run.chars().collect();
        if chars.len() < n {
            continue;
        }
        // Cross-tokenizer separation lives in the [`NgramHash`]
        // schema, not a byte-level namespace — no `"N:"` prefix.
        // Different n values produce different-length inputs to
        // Blake3 for ASCII text, so 2-grams and 3-grams mixed in
        // one index don't collide at the hash level either.
        let mut gram = String::with_capacity(n * 4);
        for window in chars.windows(n) {
            gram.clear();
            for &c in window {
                gram.push(c);
            }
            out.push(Value::<NgramHash>::new(*blake3::hash(gram.as_bytes()).as_bytes()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_whitespace() {
        let tokens = hash_tokens("one two three");
        assert_eq!(tokens.len(), 3);
    }

    #[test]
    fn case_insensitive() {
        let a = hash_tokens("FOO");
        let b = hash_tokens("foo");
        assert_eq!(a, b);
    }

    #[test]
    fn strips_punctuation() {
        let a = hash_tokens("hello,");
        let b = hash_tokens("hello");
        assert_eq!(a, b);
    }

    #[test]
    fn preserves_duplicates() {
        let tokens = hash_tokens("foo bar foo");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], tokens[2]);
    }

    #[test]
    fn drops_empty_tokens() {
        // Pure-punctuation tokens disappear after trimming.
        let tokens = hash_tokens("foo  ---  bar");
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn stable_hash() {
        // Regression guard: the Blake3 encoding of "hello" must
        // not drift across crate versions.
        let tokens = hash_tokens("hello");
        let expected = *blake3::hash(b"hello").as_bytes();
        assert_eq!(tokens[0].raw, expected);
    }

    #[test]
    fn ngram_empty_n_returns_nothing() {
        assert!(ngram_tokens("anything", 0).is_empty());
    }

    #[test]
    fn ngram_skips_short_runs() {
        // "hi" (len 2) drops for n=3.
        assert!(ngram_tokens("hi", 3).is_empty());
    }

    #[test]
    fn ngram_counts() {
        // "foxes" -> fox, oxe, xes = 3 trigrams
        assert_eq!(ngram_tokens("foxes", 3).len(), 3);
        // "foxes" -> fo, ox, xe, es = 4 bigrams
        assert_eq!(ngram_tokens("foxes", 2).len(), 4);
    }

    #[test]
    fn ngram_case_insensitive() {
        let a = ngram_tokens("FOX", 3);
        let b = ngram_tokens("fox", 3);
        assert_eq!(a, b);
    }

    #[test]
    fn ngram_does_not_cross_punctuation() {
        // "foo-bar" splits on '-' so the tri-gram window doesn't
        // span the boundary (no "oo-" or "o-b" grams).
        let dashed = ngram_tokens("foo-bar", 3);
        let spaced = ngram_tokens("foo bar", 3);
        assert_eq!(dashed, spaced);
        assert_eq!(dashed.len(), 2); // "foo" and "bar"
    }

    #[test]
    fn ngram_size_namespaced() {
        // "fo" as a bigram and "fo" as a prefix of a trigram
        // produce distinct hashes — same glyphs, different n.
        let bi = ngram_tokens("fo", 2);
        let tri = ngram_tokens("foo", 3);
        assert_eq!(bi.len(), 1);
        assert_eq!(tri.len(), 1);
        assert_ne!(bi[0], tri[0]);
    }

    #[test]
    fn bigram_tokens_basic_count() {
        // 4 words → 3 bigrams.
        assert_eq!(bigram_tokens("the quick brown fox").len(), 3);
        assert_eq!(bigram_tokens("one two").len(), 1);
        assert!(bigram_tokens("lonely").is_empty());
        assert!(bigram_tokens("").is_empty());
    }

    #[test]
    fn bigram_tokens_case_and_punctuation_normalized() {
        // Same output after lowercase + punctuation trim —
        // matches hash_tokens' normalization so bigrams and
        // single-word terms share the same term-space floor.
        let a = bigram_tokens("Hello, WORLD!");
        let b = bigram_tokens("hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn bigram_tokens_order_matters() {
        // (a, b) ≠ (b, a): bigrams encode ordered pairs.
        let ab = bigram_tokens("foo bar");
        let ba = bigram_tokens("bar foo");
        assert_ne!(ab, ba);
    }

    #[test]
    fn bigram_tokens_separated_from_hash_by_schema() {
        // Single-word `hash_tokens` output is
        // `Value<WordHash>`; bigram output is `Value<BigramHash>`
        // — different types. The compiler enforces the
        // separation; they can't accidentally share an index or
        // be swapped in a query. Byte-level collision is
        // immaterial once the schemas differ.
        let single = hash_tokens("foobar");
        let bigram = bigram_tokens("foo bar");
        assert_eq!(single.len(), 1);
        assert_eq!(bigram.len(), 1);
        // Even the raw bytes differ (NUL delimiter in the
        // bigram), but the stronger guarantee is the type-level
        // separation.
        assert_ne!(single[0].raw, bigram[0].raw);
    }

    #[test]
    fn bigram_tokens_word_boundary_preserved() {
        // "ab c" (two words) and "a bc" (two words) must produce
        // DIFFERENT bigrams even though the concatenated glyphs
        // are identical. The \0 delimiter in the namespace tag
        // is what makes this safe.
        let ab_c = bigram_tokens("ab c");
        let a_bc = bigram_tokens("a bc");
        assert_eq!(ab_c.len(), 1);
        assert_eq!(a_bc.len(), 1);
        assert_ne!(ab_c[0], a_bc[0]);
    }

    #[test]
    fn bigram_tokens_enables_phrase_match() {
        // Building a BM25 index keyed by entity id with bigram
        // terms and querying for a 2-word phrase recovers only
        // docs that contain those two words adjacently.
        use crate::bm25::BM25Builder;
        use triblespace_core::id::Id;
        use triblespace_core::value::schemas::genid::GenId;

        fn iid(byte: u8) -> Id {
            Id::new([byte; 16]).unwrap()
        }
        let mut b: BM25Builder<GenId, BigramHash> = BM25Builder::new();
        b.insert(iid(1), bigram_tokens("the quick brown fox"));
        b.insert(iid(2), bigram_tokens("fox fight club"));
        // doc 3 has `quick` + `brown` but NOT adjacent — no
        // (quick, brown) bigram.
        b.insert(iid(3), bigram_tokens("quick silver brown fox"));
        let idx = b.build();

        let phrase = bigram_tokens("quick brown");
        assert_eq!(phrase.len(), 1);
        let hits: Vec<_> = idx.query_term(&phrase[0]).collect();
        assert_eq!(hits.len(), 1);
        let mut key1 = [0u8; 32];
        key1[16..32].copy_from_slice(AsRef::<[u8; 16]>::as_ref(&iid(1)));
        assert_eq!(hits[0].0.raw, key1);
    }

    #[test]
    fn code_tokens_snake_case() {
        let t = code_tokens("parse_http_response");
        let expected = ["parse", "http", "response"]
            .iter()
            .map(|s| Value::<WordHash>::new(*blake3::hash(s.as_bytes()).as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(t, expected);
    }

    #[test]
    fn code_tokens_camel_case() {
        let t = code_tokens("parseResponseBody");
        let expected = ["parse", "response", "body"]
            .iter()
            .map(|s| Value::<WordHash>::new(*blake3::hash(s.as_bytes()).as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(t, expected);
    }

    #[test]
    fn code_tokens_acronym_boundary() {
        // HTMLParser — HTML stays together as an acronym, then
        // Parser breaks off.
        let t = code_tokens("HTMLParser");
        let expected = ["html", "parser"]
            .iter()
            .map(|s| Value::<WordHash>::new(*blake3::hash(s.as_bytes()).as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(t, expected);
    }

    #[test]
    fn code_tokens_digits_split() {
        let t = code_tokens("parseV2Request");
        let expected = ["parse", "v", "2", "request"]
            .iter()
            .map(|s| Value::<WordHash>::new(*blake3::hash(s.as_bytes()).as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(t, expected);
    }

    #[test]
    fn code_tokens_mixed_separators() {
        // Hyphens, dots, spaces all behave like snake separators.
        let a = code_tokens("foo-bar.baz qux");
        let b = code_tokens("foo bar baz qux");
        assert_eq!(a, b);
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn code_tokens_shares_terms_with_hash_tokens() {
        // Key property: the lowercase words coming out of
        // code_tokens match hash_tokens on the same word, so the
        // two tokenizers coexist in one BM25 index.
        let code = code_tokens("parseFooBar");
        let text = hash_tokens("parse foo bar");
        assert_eq!(code.len(), 3);
        assert_eq!(text.len(), 3);
        for (c, t) in code.iter().zip(text.iter()) {
            assert_eq!(c, t);
        }
    }

    #[test]
    fn code_tokens_example_in_doc() {
        // Matches the doctest.
        let t = code_tokens("parseHTMLResponse_v2");
        assert_eq!(t.len(), 5);
    }

    #[test]
    fn ngram_shared_prefix_matches_extension() {
        // The key property: "fox" and "foxes" share a trigram, so
        // a BM25 index keyed on ngram_tokens would score them
        // relative to each other — prefix / fuzzy matching for
        // free.
        let short = ngram_tokens("fox", 3);
        let long = ngram_tokens("foxes", 3);
        assert!(long.contains(&short[0]));
    }
}
