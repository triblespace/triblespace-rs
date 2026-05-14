# Query engine integration

How BM25 posting-list constraints and HNSW similarity plug into
the triblespace query engine as first-class `Constraint`s. Most
of this is shipped; sections marked **(planned)** describe the
still-open roadmap.

## Goal

A user writes a `find!` that composes BM25, HNSW, and arbitrary
`pattern!` clauses in one engine pass:

```rust
let hits: Vec<(Id,)> = find!(
    (paper: Id),
    temp!(
        (emb),
        and!(
            bm25.matches(paper, &graph_terms, 0.0),
            pattern!(&kb, [{ ?paper @ attrs::paper_embedding: ?emb }]),
            hnsw_view.similar_to(query_handle, emb, 0.8),
        )
    )
)
.collect();
```

— and the engine honours its normal cardinality-driven join
reordering, `or!` composition, `exists!` short-circuiting, etc.
Search stops being a bolt-on retrieval API and becomes part of
the query algebra. See `examples/hybrid_search.rs` for the full
runnable form.

## What a `Constraint` needs to provide

From `triblespace::core::query::Constraint`:

1. **Variables it touches** — returned as a `VariableSet` so
   the engine knows which assignments the constraint constrains.
2. **Cardinality estimate** per bound-variable combination. The
   engine uses this to reorder joins — smallest estimated
   result-set wins.
3. **Propose values for one variable given the others bound.**
   For BM25: propose `doc` given a pinned `term`; propose
   `score` given `doc + term`; etc.
4. **Confirm a fully-bound tuple.** Given all variables bound,
   yes or no.

## BM25 as a `Constraint`

One constraint shape: `BM25Filter<S>`, generic over the doc
schema. Same shape on both naive `BM25Index<D, T>` and succinct
`SuccinctBM25Index<D, T>`.

### `matches(doc, &terms, score_floor)`

```rust
let c: BM25Filter<D> = idx.matches(doc, &terms, score_floor);
```

One variable (`doc: Variable<D>`), one slice of typed terms
`&[Value<T>]`, and one `score_floor: f32` parameter. Binds
`doc` to documents whose summed BM25 score across every term
in `terms` is at least `score_floor`. The engine can propose
matching docs, or confirm a bound doc against the filter set.

**Cardinality** = number of docs that clear the floor — exact,
computed once at construction.

Aggregation runs once at construction time: walk every term's
posting list, sum scores into a `HashMap<doc, f32>`, drop docs
below the floor, keep the doc keys. Triblespace has no
"arithmetic sum of bound variables" primitive, so this
pre-materialisation is the cleanest path; the resulting
constraint is small (`Vec<RawValue>`, one entry per matching
doc) and the engine path needs no further lookups.

Typical calls (the `_text` forms tokenise the query string
internally via `hash_tokens` and are available on `WordHash`-keyed
indexes — the default):

```rust
// "Filter only" — score_floor = 0.0 includes any matching doc.
let docs: Vec<(Id,)> = find!(
    (book: Id),
    and!(
        idx.matches_text(book, "graph search algorithms", 0.0),
        pattern!(&kb, [{ ?book @ literature::author: &target_author }]),
    ),
)
.collect();

// "Relevance threshold" — only docs whose summed BM25 ≥ floor.
let docs: Vec<(Id,)> = find!(
    (book: Id),
    idx.matches_text(book, "graph search algorithms", 1.5),
)
.collect();

// For other tokeniser flavours (bigrams, n-grams, code tokens) or
// when reusing the same token slice across many `score` calls,
// the explicit form takes a pre-tokenised `&[Value<T>]`:
let tokens = hash_tokens("graph search algorithms");
let docs: Vec<(Id,)> = find!(
    (book: Id),
    idx.matches(book, &tokens, 0.0),
)
.collect();
```

The schema-typed term values (`Value<WordHash>`,
`Value<BigramHash>`, etc.) keep the compiler enforcing that the
right tokenizer's output reaches the right index.

### `score(&doc, &terms) -> f32`

```rust
let s = idx.score(&doc.to_value(), &tokens);
```

Recompute helper for ranking. Returns the summed BM25 score for
`doc` across `terms` — same number `matches` used internally,
exposed as a plain function. Lossless f32 (no engine-side
equality bookkeeping); on the succinct index the score reflects
the stored u16 quantisation but at f32 precision.

Typical use after `matches` filters through the engine:

```rust
let mut ranked: Vec<(Id, f32)> = find!(
    (doc: Id),
    idx.matches(doc, &tokens, 0.0)
)
.map(|(d,)| (d, idx.score(&d.to_value(), &tokens)))
.collect();
ranked.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
```

Same pattern as HNSW: filter on a fixed floor, recompute the
precise score afterwards if you need it for ranking. Score is
never a bound query variable — quantisation bookkeeping doesn't
reach the engine path, and the join planner stays tight (one
less variable per BM25 clause).

See `examples/multi_term_bm25_search.rs` for the full runnable
flow, and `BM25Filter` in the `constraint` module for the
engine-facing type.

### Reverse lookup: doc bound, term free *(planned)*

Rare: "what terms are in this doc." Requires a per-doc inverted
structure. V1 doesn't expose this — the primary join direction is
term → docs. If a caller needs the reverse, they walk the term
table themselves and filter.

### Shared `BM25Queryable` trait

Both the naive `BM25Index<D, T>` and the succinct
`SuccinctBM25Index<D, T>` implement `BM25Queryable`. The
constraints are generic over it — same constraint types, same
`find!` integration, either backend.

## HNSW as a `Constraint`

### Binary relation: `similar(a, b, score_floor)`

Two variables (`a, b: Variable<Handle<Embedding>>`) and
one fixed cosine threshold. Produced by the `similar` method on
any attached view:

```rust
let c: Similar<'_, _> = view.similar(a, b, score_floor);
```

**Semantics:** `similar(a, b, floor)` holds iff both handles
exist in the pile's blob store and `cosine(*a, *b) ≥ floor`.
The relation is symmetric (cosine is symmetric). Operationally,
at least one of `a` / `b` must be bound so the engine can walk
the HNSW graph from that side; the other side then enumerates
candidates above the floor.

### Why threshold, not top-k

Top-k is an *operational* choice — the caller decided to keep
N results after running the walk. Threshold is *semantic* — "a
cosine of ≥ 0.8 means the vectors are close enough for this
query". Semantics compose through the engine; operational
knobs don't. If a caller needs top-k, they walk on their side
and slice.

### Why fixed score (not a variable)

Score being free would force the index to report every visited
node during ef-search with its cosine, and the engine would
join on quantized scores. The caller almost never gets a
meaningful `score` from somewhere else in the query — and if
they want the exact similarity for a specific `(a, b)` pair,
fetching both embedding blobs and computing cosine directly is
one line and unaffected by u16 quantization.

### Cardinality

- One side bound: exact — run the walk, return the candidate
  count.
- Neither side bound: `usize::MAX` — the engine should order
  other constraints first. (Returning `None` would flag the
  variable as unconstrained.)

### Shared `SimilaritySearch` trait

`AttachedHNSWIndex`, `AttachedFlatIndex`, and
`AttachedSuccinctHNSWIndex` all implement `SimilaritySearch`
with two methods: `neighbours_above(handle, floor)` and
`cosine_between(a, b)`. Both are infallible at the trait
boundary — fetch failures fail-open as "no match" (empty vec /
`None`) because the engine's propose/confirm hooks have no
error channel.

### Convenience: `similar_to(probe, var, score_floor)`

```rust
let c: SimilarTo = view.similar_to(probe, var, score_floor);
```

Unary sugar for the common "search from a known handle" case.
Equivalent to
`temp!((a), and!(a.is(probe), view.similar(a, var, floor)))`,
but the probe is pinned on the call site — no temp variable
allocation, no `.is()` dance. Walks the index once at
construction and caches the above-threshold set; subsequent
engine calls iterate the cache.

Use when you already hold the query handle (which covers the
vast majority of callers). Keep [`Similar`] for the cases
where both sides are genuinely variables — multi-probe
clustering, symmetric self-joins, etc.

[`Similar`]: #binary-relation-similara-b-score_floor

## Combinators callers actually write

These compose naturally from the primitive constraints:

```rust
// Hybrid: title mentions 'graph' AND embedding close to query.
find!(
    (paper: Id),
    temp!((emb),
        and!(
            bm25.matches(paper, &graph_terms, 0.0),
            pattern!(&kb, [{ ?paper @ attrs::paper_embedding: ?emb }]),
            hnsw.similar_to(query_handle, emb, 0.8),
        )
    ),
)

// "Fragments citing X that also mention 'typst'."
find!(
    (doc: Id),
    and!(
        bm25.matches(doc, &[id_as_term(x)], 0.0),
        bm25.matches_text(doc, "typst", 0.0),
    ),
)

// "Similar to query, restricted to kind tag."
find!(
    (doc: Id),
    temp!((emb),
        and!(
            pattern!(&kb, [
                { ?doc @ metadata::tag: &kind },
                { ?doc @ attrs::doc_embedding: ?emb },
            ]),
            hnsw.similar_to(query_handle, emb, 0.7),
        )
    ),
)
```

The library does not provide `top_k` / `sort_by_score` in the
engine. Ordering is operational — callers collect the iterator
and slice. Matches the "unordered queries" tenet.

## Handle resolution

The constraint borrows from a specific index value (naive or
reloaded from a blob). Typical flow:

```rust
let handle: Value<Handle<SuccinctBM25Blob>> =
    load_current_index_handle(&kb)?;
let reader = pile.reader()?;
let idx: SuccinctBM25Index =
    reader.get::<SuccinctBM25Index, SuccinctBM25Blob>(handle)?;
let c = idx.matches(doc, &terms, 0.0);
```

`idx` owns the data; `c` borrows it for the duration of the
query pass. A later rebuild produces a new handle; the next
query picks it up by loading the updated handle.

## Open questions

1. **Reverse lookups.** See the BM25 section; currently
   unindexed, reported via doc-side walk if ever needed.
2. **Async / deferred index loading.** Large blobs are
   mmap-backed via `anybytes::Bytes` already; a `Bytes::view`
   failure happens at load time, not at constraint use time.
3. **String-input `matches`.** Resolved: `WordHash`-keyed
   indexes ship `matches_text(doc, "...", floor)` and
   `score_text(&doc, "...")` sugar that tokenises via
   `hash_tokens` internally. The general form `matches(doc,
   &terms, floor)` stays the entry point for `bigram_tokens` /
   `ngram_tokens` / `code_tokens` and for callers that want to
   hand-hoist tokenisation across many `score` calls.

## Non-goals (v1)

- `top_k` / `sort_by_score` combinators. Callers slice.
- Hybrid score as a first-class bound variable. Callers write
  the linear combination in Rust.
- Live incremental updates. Rebuild-and-replace only.
- Cross-language bindings. Rust-native only.
