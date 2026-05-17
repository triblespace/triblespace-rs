# Changelog

All notable changes to `triblespace-search`.

Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.41.2] - 2026-05-17

Lock-step bump alongside the address-symmetry work in
`triblespace-net` / `trible`. No source changes in
`triblespace-search`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.1] - 2026-05-17

Lock-step bump alongside the EndpointTicket-everywhere work
in `triblespace-net` / `trible`. No source changes in
`triblespace-search`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.0] - 2026-05-16

Lock-step bump alongside the iroh 0.98 family upgrade in
`triblespace-net`. No source changes in `triblespace-search`.
See the workspace [`../CHANGELOG.md`](../CHANGELOG.md) for
the full release notes.

## [0.38.0] - 2026-05-07

Lock-step bump alongside the team-rooted-gossip release in
`triblespace-net` / `trible`. No source changes in
`triblespace-search`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.37.0] - 2026-05-06

First crates.io release. Aligned with the workspace minor;
re-exported at `triblespace::search` behind the `search`
feature. Entries below cover the canonical-bytes architectural
shift and the API-affordance tightening that brought the public
surface to a shape we're willing to commit to.

### `candidates_above` hidden from rustdoc — `similar_to` is the idiomatic surface

`Attached*Index::candidates_above` is the underlying graph walk
that `Similar` / `SimilarTo` constraints wrap. Two near-equivalent
public APIs side-by-side risked teaching new users to bypass the
engine — the constraint version composes with `pattern!` /
`bm25.matches` / range filters in one `find!` pass; the leaf
returns a `Vec<EmbHandle>` and forces them to thread it through
the next stage by hand.

Three changes:

1. **`#[doc(hidden)]`** on `candidates_above` in all three impls
   (`AttachedHNSWIndex`, `AttachedFlatIndex`,
   `AttachedSuccinctHNSWIndex`). Still callable — tests
   (`scale_smoke`, `pile_roundtrip`, `find_macro`) and
   benchmarks (`query_latency`) keep using it as a correctness
   oracle and a "walk-only" timing surface — but rustdoc no
   longer advertises it. The visible similarity surface in the
   rendered docs is `similar_to(probe, var, floor)` /
   `similar(a, b, floor)`.

2. **`compose_hnsw_and_pattern` example** rewritten to use the
   engine path even for the standalone-similarity case. Same
   results, idiomatic shape — copy-paste teaches the right
   pattern.

3. **Doc language** in this CHANGELOG and method docstrings
   recast `candidates_above` as "leaf for tests/benchmarks";
   `similar_to` is "the production surface."

No behaviour change; same wire format; same query results. Pure
API-affordance reshaping so the engine path is the obvious
default.

### `SuccinctPostings::build_with_into` is now single-pass — caller supplies sizing scalars

`build_with_into` previously walked the closure twice per term:
once to discover `total` (sum of posting counts) and `max_score`
(quantization scale), then again to write the bit-packed
CompactVectors. The closure-must-be-deterministic-across-calls
contract was a usability footgun, and for our one in-tree
caller — `SuccinctBM25Index::from_builder` — pass 1 was
duplicate work the BM25 path could have computed cheaply.

Refactored: `build_with_into` now takes `total: usize` and
`max_score: f32` as caller-supplied parameters and invokes the
closure exactly once per term during the byte-write pass. The
closure no longer has to be deterministic across calls.

`SuccinctBM25Index::from_builder` precomputes:

- `total = term_to_tfs.values().map(|m| m.len()).sum()` — a
  free walk over outer-HashMap sizes, no inner traversal.
- `max_score` = a per-term scan that runs the BM25 score formula
  to find the corpus max. Same per-posting score eval as before,
  but skips the per-term sort that `build_with_into`'s pass 1
  used to do — net work drops from "score-eval×2 + sort×2" to
  "score-eval×2 + sort×1" (the write pass still sorts to produce
  ascending-doc-code postings).

The standalone `SuccinctPostings::build_with` keeps its
two-pass closure contract — it does the sizing pass internally
before delegating to `build_with_into`, since callers using the
standalone path don't have side knowledge of their data sizes.

Measured at 50 k docs / 20 k vocab:

| phase                       | before  | after  |
| :-------------------------- | ------: | -----: |
| `BM25Builder::build` time   | ~1024 ms | **~939 ms** |
| Peak memory                 | 209 MiB  | 209 MiB (unchanged) |
| Output bytes                | identical | identical |

The ~10-15% speedup matches the saved per-term sort. Output is
byte-identical — `scale_smoke` and `pile_roundtrip` integration
tests both pass without modification.

A `debug_assert_eq!(pos, total, …)` inside `build_with_into`
catches caller miscounts in test builds; release builds rely on
the caller to get `total` right (bit-packing is silent on
overflow).

### `to_bytes` / `try_from_bytes` retired — `pub bytes: Bytes` is the surface

The wrapper methods predated the canonical-bytes refactor. With
the index *being* its blob, the right shape mirrors
[`triblespace_core::SuccinctArchive`] — a single `pub bytes:
Bytes` field, no accessor methods, and a single typed loader
([`TryFromBlob<…Blob>`]).

What changed:

- `SuccinctBM25Index::to_bytes(&self) -> Vec<u8>` and
  `SuccinctHNSWIndex::to_bytes(&self) -> Vec<u8>` deleted —
  they were just `self.bytes.as_ref().to_vec()`, a wasteful
  20 MB memcpy at 50 k docs that defeated the canonical-bytes
  win at the user-facing API. Callers that genuinely need an
  owned `Vec<u8>` write that themselves; everything else uses
  the `bytes` field directly.
- `SuccinctBM25Index::try_from_bytes(&[u8])` and
  `SuccinctHNSWIndex::try_from_bytes(&[u8])` deleted — naming
  evoked `zerocopy::TryFromBytes` semantics they didn't satisfy
  (they actually allocated a `Bytes::from_source(buf.to_vec())`
  before parsing). Callers wrap raw bytes in
  `Blob::new(bytes)` and route through `TryFromBlob<…Blob>`,
  matching `SuccinctArchive` exactly.
- All call sites migrated:
  - `idx.to_bytes().len()` → `idx.bytes.len()`
  - `let bytes = idx.to_bytes();
    Self::try_from_bytes(&bytes)` →
    `let blob = Blob::new(idx.bytes.clone());
     Self::try_from_blob(blob)`
  - `assert_eq!(a.to_bytes(), b.to_bytes())` →
    `assert_eq!(a.bytes.as_ref(), b.bytes.as_ref())`
- Updated 5 tests, 3 examples (`query_demo`,
  `blob_sizes_at_scale`, `peak_build_memory`), and one doctest.
  `peak_build_memory` now measures `(&idx).to_blob()` instead
  of `to_bytes`, reporting **0 B peak at every scale** — the
  canonical-bytes win finally visible at the user-facing API.
- `blob_sizes_at_scale`'s manual `keys_len_from_header` byte
  scrape (which decoded the now-retired custom 264 B header)
  is replaced with `keys_len_from_blob(&idx)` reading the
  suffix-meta directly via `idx.meta()`.

158 tests still pass; all serialization round-trips go through
`ToBlob` / `TryFromBlob` end-to-end.

### `FixedBytesTable<N>` retired — switch to `View<[[u8; N]]>` directly

The wrapper type added nothing on top of what `View<[[u8; N]]>`
already gives via slice methods. With the canonical-bytes
refactor, the only surface that mattered was reading rows back
through a `SectionHandle<[u8; N]>`, and `SectionHandle::view(&bytes)`
returns `View<[[u8; N]]>` directly. The `View` derefs to
`[[u8; N]]`, so `binary_search`, `get`, `len`, `is_empty`,
`to_vec` are all native slice methods — no wrapper-type
forwarding.

What changed:

- `SuccinctBM25Index.terms` and `SuccinctHNSWIndex.handles`
  field types switch from `FixedBytesTable<32>` to
  `View<[[u8; 32]]>`. All call sites
  (`self.terms.binary_search(&term.raw)`,
  `self.handles.len()`, etc.) keep working — the View-deref
  routes them through the same slice methods.
- `pack_byte_table::<N>(sections, rows) -> SectionHandle<[u8; N]>`
  is the lone remaining helper — a small free function around
  `sections.reserve::<[u8; N]>` + `copy_from_slice` + `freeze`.
- `from_bytes` paths use `meta.terms.view(&bytes)?` /
  `meta.handles.view(&bytes)?` directly — the existing
  anybytes API.
- `FixedBytesTable<N>` struct + 4 standalone-API methods
  (`build`, `build_into`, `from_bytes`, `from_section_handle`)
  + 5 internal methods (`len`, `is_empty`, `get`,
  `binary_search`, `to_vec`) all deleted. Two new
  `pack_byte_table` round-trip tests cover the new helper.

Test count: 162 → 158 (5 `fixed_table_*` tests removed,
2 `pack_byte_table_*` tests added; one doctest gone with the
struct). The pile_roundtrip / scale_smoke / engine_integration
suites all keep passing — the refactor is purely a
representation switch, no behavior change.

### Canonical-bytes pattern for `SuccinctHNSWIndex` — `to_blob` is O(1)

Same shape as the BM25 commit: `SuccinctHNSWIndex` gains a
`bytes: Bytes` field, `from_naive` builds every section
(handles, graph) into one shared `anybytes::ByteArea`, the new
`SuccinctHNSWMeta` header sits as a typed suffix-section, and
the area is frozen exactly once. `ToBlob<SuccinctHNSWBlob>` is
now a refcounted `Bytes::clone` instead of a full graph rebuild.

#### Wire format change — schema id rotated

The custom 128 B header is gone (no more
`SH25_HEADER_LEN`-shaped scalar+offset preamble). The new
layout:

```
[ handles section       ]   FixedBytesTable<32> (in shared area)
[ graph sections        ]   2 × CompactVector (in shared area)
[ suffix meta           ]   SuccinctHNSWMeta (zerocopy-readable, 128 B)
```

`SuccinctHNSWBlob` schema id rotates from
`A96890DE5F85A4F2285C365549B21BC2` to
`8DF997D25C15B73EDCEE9E08076F251E` (minted via `trible genid`).

#### Internal changes

- `SuccinctHNSWMeta` zerocopy struct (size statically asserted
  at 128 bytes — `_pad: [u8; 10]` rounds to a multiple of 8).
- `SuccinctGraphMeta` reordered (largest-alignment-first) and
  gained zerocopy derives so it nests cleanly inside
  `SuccinctHNSWMeta`.
- `SuccinctHNSWIndex::from_bytes(meta, bytes)` — shared-bytes
  view reconstruction, used by both `from_naive` and the
  `TryFromBlob` path.
- `SuccinctHNSWIndex::meta(&self)` — `O(1)` zerocopy
  suffix-view of the canonical bytes.
- `to_bytes` simplified to `self.bytes.as_ref().to_vec()`.
- Dead code removed: `CompactVectorMetaOnDisk` (last user was
  the retired HNSW custom header), `SH25_HEADER_LEN`, the
  `zerocopy::IntoBytes` import.

162 tests still pass; pile_roundtrip's three round-trip tests
(BM25 round-trip, HNSW round-trip, shared-embedding-blob
verification) all pass through the new path.

### Canonical-bytes pattern for `SuccinctBM25Index` — `to_blob` is O(1)

Major architectural shift to mirror `triblespace-core`'s
`SuccinctArchive` shape: every section (`keys`, `doc_lens`,
`terms`, `postings`) lives in **one** shared `anybytes::ByteArea`,
the new `SuccinctBM25Meta` header sits as a typed suffix-section,
and the area is frozen exactly once. The resulting `bytes: Bytes`
field on `SuccinctBM25Index` *is* the blob. Persistence is then
free.

Before:
```rust
fn to_blob(self) -> Blob<SuccinctBM25Blob> {
    Blob::new(Bytes::from_source(self.to_bytes()))  // rebuild every section
}
```

After:
```rust
fn to_blob(self) -> Blob<SuccinctBM25Blob> {
    Blob::new(self.bytes)  // O(1) — refcounted handover
}
```

Measured on the `peak_build_memory` example at 50 k docs:

| phase                       | before  | after   |
| :-------------------------- | ------: | ------: |
| `to_bytes` time             | 274 ms  |   2 ms  |
| `to_bytes` peak             | 20.1 MB |  20.1 MB (just a memcpy of the canonical bytes) |
| `ToBlob` (refcounted clone) | n/a     |  ~ns    |

Streaming the rebuild was a transitional fix; the canonical-bytes
pattern eliminates the rebuild entirely. `to_bytes()` is now just
`self.bytes.as_ref().to_vec()` — kept around for callers that
need an owned `Vec<u8>`, but the recommended persistence path is
`ToBlob`'s O(1) handover.

#### Wire format change — schema id rotated

The custom 264 B header is gone. The new layout:

```
[ keys section          ]   CompressedUniverse (in shared area)
[ doc_lens section      ]   CompactVector (in shared area)
[ terms section         ]   FixedBytesTable<32> (in shared area)
[ postings sections     ]   3 × CompactVector (in shared area)
[ suffix meta           ]   SuccinctBM25Meta (zerocopy-readable)
```

`SuccinctBM25Blob` schema id rotates from
`5A1EF3FFD638B15E3EBEAA1E92660441` to
`DA527A8FF09A3709B2AC6425CD5AF7A8` (minted via `trible genid`).
The compiler treats the new id as a different type, so any
mismatched-layout deserialization is a type error rather than a
runtime failure.

#### What's still on the table

`SuccinctHNSWIndex` still uses the custom-header `to_bytes` path
(`SH25_HEADER_LEN = 128`). The same canonical-bytes refactor
applies — that's the next iteration. The `_into` section
builders (`SuccinctGraph::build_into`, `FixedBytesTable::
build_into`, `SuccinctPostings::build_with_into`) are already in
place from the previous commit, so the scaffolding is ready.

#### Internal changes

- `SuccinctBM25Meta` zerocopy struct (`FromBytes + KnownLayout +
  Immutable`) — embedded directly as a `reserve::<SuccinctBM25Meta>(1)`
  section in the area, read back via `Bytes::view_suffix`.
- `SuccinctPostingsMeta` reordered (largest-alignment-first)
  and gained zerocopy derives so it nests cleanly inside
  `SuccinctBM25Meta`.
- `SuccinctBM25Index::from_bytes(meta, bytes)` — shared-bytes
  view reconstruction, used by both the builder and the
  `TryFromBlob` path.
- `FixedBytesTable::from_section_handle(bytes, handle)` — slice
  the row table out of a shared area's bytes via the
  `SectionHandle<[u8; N]>` `build_into` returned.
- Dead code removed: `CompressedUniverseMetaOnDisk` (only used
  by the retired custom header), `SUCCINCT_HEADER_LEN`.

### `peak_build_memory` example — tracking-allocator validation of streaming refactors

New `examples/peak_build_memory.rs` measures peak heap allocation
through `BM25Builder::build_naive`, `BM25Builder::build`, and
`SuccinctBM25Index::to_bytes` via a process-global tracking
allocator (CAS-loop on a `PEAK` atomic). Each phase resets the
peak watermark to current resident, runs the operation, and
reports `peak_during - baseline`.

Observed numbers at 50 k docs / 20 k vocab / 96 tokens-per-doc
(release build, single laptop run):

| phase                                     | peak +     |
| :---------------------------------------- | ---------: |
| `BM25Builder::build_naive`                | 203.7 MiB  |
| `BM25Builder::build` (streaming succinct) | 209.4 MiB  |
| `SuccinctBM25Index::to_bytes`             |  20.1 MiB  |

Honest takeaway: the build-side streaming win is *masked* at this
scale — `term_to_tfs: HashMap<RawInline, HashMap<u32, u32>>`
dominates at ~150 MiB, dwarfing the
`Vec<Vec<(u32, f32)>>` intermediate the streaming refactor
removed (~24 MiB at 50 k, ~144 MiB at 100 k+ where the
optimization actually starts mattering). The to_bytes peak,
however, bottoms out at the SB25 blob size — what remains is
effectively the output buffer; the streaming refactor erased
the triple-allocation pattern that previously inflated this
phase by ~50 MiB at 50 k scale.

The example is a measurement aid for "how much does this
optimization actually save" claims, not a regression test —
`tests/scale_smoke.rs` covers byte-identity of the layout.

### `to_bytes` re-serialization streamed — drops triple-allocation pattern

Both blob types now stream their re-serialization paths through
the same closure-based build APIs `from_builder` already uses.
Specifically:

- `SuccinctBM25Index::to_bytes`: postings re-serialization routes
  through `SuccinctPostings::build_with` (drops the
  `Vec<Vec<(u32, f32)>>` re-collection — ~144 MB peak drop at
  100 k docs); terms section streams rows from the existing
  `FixedBytesTable<32>` view straight into the output buffer
  with `terms_len = n_terms × 32` computed up front (drops a
  9.6 MB `Vec<u8>` round-trip at 300 k terms).
- `SuccinctHNSWIndex::to_bytes`: handles section streams from
  the existing view directly into the output buffer (drops
  three redundant copies — `Vec<RawInline>` build input, a
  fresh `FixedBytesTable<32>::build` ByteArea, and a
  `.as_ref().to_vec()` flat copy — collectively ~10 MB at
  100 k nodes).

The serialized layouts are bit-identical to the previous paths;
the existing `pile_roundtrip` and `scale_smoke` integration
tests all pass unchanged. This closes the loop on the streaming
`build_with` work landed in the previous commit, applying the
same pattern to re-serialization too.

### Streaming `SuccinctPostings::build_with` — drops peak build memory

`SuccinctPostings` gains a closure-based builder that materializes
one term's posting list at a time into a reused buffer instead of
requiring `Vec<Vec<(u32, f32)>>` for the whole corpus upfront. The
existing slice-based `build` is now a thin wrapper over `build_with`
so old call sites keep working.

`SuccinctBM25Index::from_builder` (the `BM25Builder::build` path)
routes through `build_with`. Memory profile at 100 k docs / 300 k
terms / Heaps-law vocabulary: peak temporary vec drops from ~144 MB
(every term's postings materialized at once) to ~400 KB (largest
single term). The intermediate `lists` allocation in `from_builder`
is gone.

The streaming closure is invoked twice per term: once during the
size + max-score pass, once during the byte-write pass. The contract
requires deterministic output across invocations — the
`build_with_streaming_matches_lists_build` regression test locks
byte-for-byte equivalence with the legacy `build(&lists, n)` path.

Test count: 161 → 162 (one new test on the determinism contract).

### BM25 redesign: `matches` filter + `score` recompute, no score variable

Major API simplification, aligning BM25 with HNSW's "filter on a
fixed floor, recompute score afterwards" pattern. Three constraints
(`docs_containing`, `docs_and_scores`, `bm25_query`) collapse to one:

```rust
// Before:
idx.docs_containing(doc, term)              // 1 var, no score
idx.docs_and_scores(doc, score, term)       // 2 vars, score bound
idx.bm25_query(doc, score, &terms)          // 2 vars, summed score

// After:
idx.matches(doc, &terms, score_floor)       // 1 var, score is a filter
idx.score(&doc.to_inline(), &terms) -> f32   // recompute precisely after
```

`score_floor = 0.0` recovers `docs_containing` semantics — BM25 is
non-negative, so `>= 0.0` matches every doc in at least one posting
list. `score_floor > 0.0` is "relevance threshold" filtering, which
the old API couldn't express without the bound score variable.

What this kills:
- `BM25ScoredPostings`, `BM25MultiTermScored`, `DocsContainingTerm`
  constraint types — collapsed into one `BM25Filter<S>`.
- The `score: Variable<F32LE>` plumbing in every constraint — score
  isn't a join variable anymore. One less variable per BM25 clause
  in the engine planner; no more bidirectional propose-by-score.
- `BM25Queryable::score_tolerance` trait method — quantisation
  bookkeeping doesn't reach the engine path. The inherent
  `SuccinctBM25Index::score_tolerance` helper stays for callers
  comparing recomputed scores against an external reference.
- `HashSet<u32>` score bit-pattern dedup (Cartesian-blowup
  avoidance) — not needed when score isn't a variable.
- `query_term_ids` / `query_multi_ids` GenId-specific shortcuts —
  callers do `Id::try_from_inline(v).unwrap()` once after the typed
  `query_term` if they want `Id` instead of `Inline<GenId>`.

The constraint module's surface is now one structural shape on each
side: `BM25Filter` for BM25, `Similar` (+ `similar_to` sugar) for
HNSW. Both follow the same rule: doc/handle is the only bound
variable, score is a fixed parameter.

Test count: 122 → 119 lib (3 score-as-variable tests removed,
2 floor + score-helper tests added); integration tests likewise
re-shaped around the new pattern.

### `BM25Builder::new` merged with `::typed` — one constructor

`BM25Builder::typed` was the generic constructor over `<D, T>`;
`BM25Builder::new` was a sibling only on `<GenId, WordHash>`
that called `typed()` internally. In practice, both `D` and `T`
are almost always inferred from downstream `insert` calls
(`&Id → ToEncoded<GenId>` pins `D`, `hash_tokens → Vec<Inline<WordHash>>`
pins `T`), so the specific-shape `new` was just sugar for the
common case.

Merged into one `new()` on the generic impl. Call sites that
can't infer (empty builders, turbofish on the variable) now
read `BM25Builder::<D, T>::new()` instead of
`BM25Builder::<D, T>::typed()`. The default-typed impl and the
specific `Default` impl are gone; `Default` is now generic over
`<D, T>` with the struct defaults.

### `similar_to(probe, var, score_floor)` — unary similarity sugar

Every similarity caller in the crate was writing the same
ceremony:

```rust
temp!((anchor), and!(
    anchor.is(query_handle),
    view.similar(anchor, var, floor),
))
```

That's `compose_hnsw_and_pattern`, `hybrid_search`,
`faculty_wiki_search` (before the switch to BM25-only query),
the `Similar` doctest, the find_macro test — every caller.

Collapsed into a single method call:

```rust
view.similar_to(query_handle, var, floor)
```

- New `SimilarTo` unary constraint in the `constraint` module.
  Candidate set pre-materialised once at construction from the
  pinned `probe` handle; `propose` / `confirm` / `satisfied`
  iterate the cache.
- Method added on `AttachedHNSWIndex`,
  `AttachedFlatIndex`, and `AttachedSuccinctHNSWIndex`.
- Binary [`Similar`] stays the primitive — `similar_to` is
  sugar for the case where the probe is known at constraint
  construction. Keep the binary form for multi-probe
  clustering, symmetric self-joins, etc.
- Examples + find_macro test + QUERY_ENGINE_INTEGRATION doc
  all flipped to the convenience. Doctest count 14 → 15.

### Higher-level BM25 query: `bm25_query(doc, score, &terms)`

Closes the "two-level query API" sketch — a multi-term
bag-of-words BM25 constraint that binds `doc` + the summed BM25
score across every query term, joinable with `pattern!` /
similarity / other BM25 clauses through the engine.

- `BM25Index::bm25_query(doc, score, &terms)` and
  `SuccinctBM25Index::bm25_query(doc, score, &terms)` produce a
  new `BM25MultiTermScored<D>` constraint. Callers typically
  feed the tokens through `hash_tokens` / `bigram_tokens` —
  the schema-typed term values keep the right tokenizer
  flavour paired with the right index.
- Aggregation (per-term posting lookup + score sum) happens
  once at construction time; triblespace has no "arithmetic
  sum of bound variables" primitive, so pre-materialising the
  `(doc, summed_score)` table is the cleanest way to expose
  the result as a constraint.
- `BM25ScoredPostings`-shaped engine behaviour: `estimate` =
  matching-doc count, `propose`/`confirm`/`satisfied` honour
  bound `doc` / `score` on either side, `score` proposals
  dedupe by bit-pattern, and the succinct backend's widened
  `score_tolerance` flows through automatically.

### HNSW redesign: handle-keyed, binary-relation similarity

Major API shift for HNSW / Flat indexes, aligning the shape with
TribleSpace taste:

- **Doc keys removed.** `HNSWBuilder`, `HNSWIndex`, `FlatBuilder`,
  `FlatIndex`, `SuccinctHNSWIndex` (and their `Attached*`
  wrappers) no longer carry a `D` generic or a separate doc-key
  table. Each node **is** a `Handle<Embedding>` — the
  caller's mapping from doc → embedding lives as a trible they
  own, not as a shadow datamodel inside the index.
- **New insert signature:** `HNSWBuilder::insert(handle, vec)` /
  `FlatBuilder::insert(handle)`. No doc-key argument.
- **Similarity is a binary relation.** The four old constraint
  types (`SimilarToVector`, `SimilarToVectorScored`,
  `SimilarToVectorHNSW`, `SimilarToVectorHNSWScored`) are
  replaced by a single [`Similar<'_, I>`][s] constraint
  produced by the `similar(a, b, score_floor)` method on each
  attached view. Both `a` and `b` are `Variable<Handle<  Embedding>>`; at least one must be bound at query time. The
  relation is symmetric (cosine is symmetric), approximate
  through HNSW.
- **Score is fixed, not a variable.** `score_floor: f32` is a
  query parameter pinned at constraint construction. Callers
  who need the exact score fetch both embeddings and compute
  cosine directly (no u16 quantization, no dedupe gymnastics).
- **`SimilaritySearch` trait** unifies `AttachedHNSWIndex`,
  `AttachedFlatIndex`, and `AttachedSuccinctHNSWIndex` behind
  the constraint — two methods (`neighbours_above`,
  `cosine_between`), infallible at the trait boundary (fetch
  failures fail-open as "no match" per engine protocol).
- **`candidates_above(handle, floor)`** is the leaf graph-walk
  the `Similar` / `SimilarTo` constraints wrap. Available on
  every attached view but `#[doc(hidden)]` — production callers
  go through the engine via `similar_to(...)` inside `find!` so
  the result composes with other constraints (BM25, pattern,
  range) in one engine pass. The leaf is for tests
  (cross-backend correctness oracles) and benchmarks (timing
  the walk in isolation). Bounded by `ef_search` (default 200,
  override with `.with_ef_search(n)`).
- **Blob schema rotations.** `SuccinctHNSWBlob::ID` rotates to
  `A96890DE5F85A4F2285C365549B21BC2` (retires
  `27D71A473EF22DA4D916F61810AC5D86`) to reflect the
  handles-only blob layout — no keys section, header shrinks
  from 144 B to 128 B.
- **Embedding implements `ConstDescribe`** so callers can
  declare `Handle<Embedding>` attributes via the
  `attributes!` macro (see the updated `compose_hnsw_and_pattern`
  / `hybrid_search` examples).

[s]: triblespace_search::constraint::Similar

### Blob types (the shipped surface)

- **`SuccinctBM25Index`** (blob schema `SuccinctBM25Blob`, id
  `68C03764D04D05DF65E49589FBBA1441`) — SB25 wire format, 236 B
  header + bit-packed body via jerky. Carries doc ids (flat),
  terms (sorted flat), doc-lens (`CompactVector`), and postings
  (three `CompactVector`s in one `ByteArea`: `doc_idx`,
  cumulative `offsets`, and u16-quantized `scores` scaled by a
  header-stored `max_score`). At 100 k docs the blob is ~86
  MiB, roughly half the naive byte-format.
- **`SuccinctHNSWIndex`** (blob schema `SuccinctHNSWBlob`, id
  `7AFE59E7F895B23F05452FF7919E12E4`) — SH25 wire format, 152 B
  header + body: doc ids, flat f32 vectors (zero-copy viewed as
  `&[f32]` via `anybytes::View`), and a CSR-shaped graph built
  from two jerky `CompactVector`s. Same greedy + ef-search as
  the naive `HNSWIndex`, producing bit-identical top-k results
  at 1 k scale.
- Both blob types implement `ToBlob` / `TryFromBlob` against
  their respective schemas, and survive a real
  `triblespace::core::repo::BlobStorePut` / `BlobStoreGet`
  round-trip (see `tests/pile_roundtrip.rs`).

### Query engine integration

- `BM25Queryable` generalizes the `DocsContainingTerm` /
  `BM25ScoredPostings` constraints over the naive
  `BM25Index<D, T>` and the succinct `SuccinctBM25Index<D, T>`.
  `SimilaritySearch` plays the same role for the attached HNSW
  / Flat / SuccinctHNSW views behind the binary-relation
  `Similar` constraint. `find!` / `pattern!` / `and!` queries
  work unmodified across backends.
- `BM25Queryable::score_tolerance()` lets the constraint's
  score-equality check widen for quantized indexes
  automatically — lossless naive path keeps `f32::EPSILON`,
  `SuccinctBM25Index` returns `max_score / 65534`.
- `BM25ScoredPostings` dedupes its score proposals by
  bit-pattern so multiple docs sharing a BM25 score don't
  expand into a Cartesian cross. Regression-locked at 1 k
  scale.

### Build-side

- `BM25Builder::build()` goes direct to `SuccinctBM25Index` in a
  single pass: sorts + dedups keys into `CompressedUniverse`
  first, then accumulates tf and scores keyed by universe code
  from the start. No insertion-order → universe-code remap, no
  per-term resort pass.
- `HNSWBuilder::build()` returns `SuccinctHNSWIndex` for the
  same "one blessed build method" ergonomic. Unlike BM25, there's
  no redundant-work win — HNSW still goes through the naive
  intermediate internally (necessary because levels are revealed
  incrementally, see node-major-vs-layer-major discussion) —
  but the public API now mirrors BM25's, and
  `SuccinctHNSWIndex::from_naive` remains available for callers
  who already hold a naive index.
- `HNSWBuilder::build_naive()` exposes the naive reference
  index (same ergonomics as `BM25Builder::build_naive`).
- Naive / oracle types (`BM25Index`, `HNSWIndex`, `FlatIndex`,
  `FlatBuilder`, `AttachedHNSWIndex`, `AttachedFlatIndex`) now
  live at `triblespace_search::testing::*`. The types are still
  physically declared in `bm25` / `hnsw` modules — their original
  paths are `#[doc(hidden)]` so rustdoc only surfaces the
  `testing::` path, signalling "reference-only, not a production
  API." The builders themselves (`BM25Builder`, `HNSWBuilder`)
  stay public at their canonical paths; the re-exports cover the
  naive forms they produce via `build_naive()`.

### Type-parameterized doc keys and terms

- `BM25Builder<D, T>` / `BM25Index<D, T>` / `SuccinctBM25Index<D, T>`
  are now generic over the doc-key schema `D` and the term
  schema `T`. Default struct types are `<GenId, WordHash>` —
  `BM25Builder::new()` works bare when the type parameters are
  inferrable from later `insert` calls (`&Id → ToEncoded<GenId>` /
  `hash_tokens → Vec<Inline<WordHash>>`). For other shapes,
  spell the schemas with a turbofish:
  `BM25Builder::<ShortString, WordHash>::new()` for a title-keyed
  index or `BM25Builder::<GenId, GenId>::new()` for
  entity-citation search.
- `insert` accepts anything that `ToEncoded<D>`-converts — pass a
  typed `Inline<D>` directly, or `&id` for the common GenId case.
  `insert_id` / `insert_value` don't exist; the single `insert`
  covers both.
- Per-tokenizer term schemas: `hash_tokens` →
  `Vec<Inline<WordHash>>`, `bigram_tokens` →
  `Vec<Inline<BigramHash>>`, `ngram_tokens` →
  `Vec<Inline<NgramHash>>`. The compiler refuses to cross-feed
  flavours into the wrong index — see
  `examples/phrase_search.rs` for the two-index pattern when a
  caller needs multiple tokenizer flavours.

The two-schema parameterization buys compile-time safety: you
can't accidentally feed ngram terms into a word-hash index,
query a title-keyed index with a GenId doc variable, or store
the wrong kind of term in a compound BM25 index. The "one
space" flexibility (mixing arbitrary 32-byte terms in one
index) is gone by default — callers who want it wrap a union
schema of their own.
- Naive `to_bytes` / `try_from_bytes` on `BM25Index` /
  `HNSWIndex` / `FlatIndex` deleted along with their
  `BM25LoadError` / `HNSWLoadError` / `FlatLoadError` types
  (~400 LOC gone). The naive indexes are reference oracles
  only; persistence is always through the succinct forms.
  `byte_size()` accessors preserve the "succinct < naive
  baseline" regression guard without materializing bytes.
- `BM25Builder::build_naive()` / `build_naive_with_threads(n)`
  keep the naive insertion-order [`BM25Index`] available as a
  correctness oracle (score comparisons in tests) and for
  benchmarking the scoring loop in isolation from jerky
  packing. The naive path still supports sharded tf accumulation
  via `std::thread::scope`; byte-identical output across
  {1, 2, 3, 4, 8} threads.
- `SuccinctBM25Index::from_naive` retired — callers that had
  `SuccinctBM25Index::from_naive(&b.build())` collapse to
  `b.build()`.
- `HNSWBuilder` with deterministic level sampling
  (`.with_seed(u64)`).

### Tokenizers (`tokens::*`)

- `hash_tokens` — whitespace + lowercase + Blake3.
- `ngram_tokens(n)` — character n-grams, `n` namespaced into
  the hash. Compose with `hash_tokens` for prefix / fuzzy
  matching.
- `code_tokens` — camelCase / snake_case / acronym / digit
  splitter. Lowercased output shares term-space with
  `hash_tokens`.
- `bigram_tokens` — word-level bigrams, `"2w:"` namespace +
  `\0` word-boundary delimiter. Compose with `hash_tokens` for
  phrase-aware retrieval.

### Schemas

- `schemas::F32LE` — 32-byte `InlineEncoding` for `f32` scores,
  used by the scored BM25 + similarity constraints.

### Examples (runnable)

- `query_demo`: text search + multi-term OR + value-as-term
  citation search.
- `compose_bm25_and_pattern`: BM25 + `pattern!` over a
  TribleSet in one `find!` / `and!`.
- `compose_hnsw_and_pattern`: vector search + `pattern!`
  composition.
- `blob_sizes_at_scale`: naive vs. SB25 blob sizes + parallel
  build speedup at 1 k / 5 k / 10 k / 50 k docs.
- `query_latency`: single-term + multi-term + HNSW latency
  p50/avg/p99 on 10 k / 50 k × 32 corpora.

### Docs

- `docs/DESIGN.md` — full design + 100 k worked example with
  measured build / size / latency numbers.
- `docs/QUERY_ENGINE_INTEGRATION.md` — constraint-trait surface.
- `docs/HNSW_GRAPH_ENCODING.md` — why the shipped CSR is
  the right HNSW graph encoding (not RING wavelet matrix) under
  current forward-only traversal, with the query patterns that
  would flip the decision.
- `docs/FACULTY_INTEGRATION.md` — worked `wiki_search.rs`
  rust-script template for consuming from faculties.

### Tests

159+ tests across unit, engine-integration (`find!`/`pattern!`
composition + regression guards), scale (1 k-doc equivalence +
size-regression + score-quantization top-10 preservation), and
`BlobStore` put/get round-trip. All pass.

## Not yet shipped

- Wavelet-matrix term table (would shrink the 9.6 MiB at 100 k;
  correctness-first is winning).
- RING-style wavelet matrix on the HNSW neighbour column (no
  win for forward-only traversal; see
  `docs/HNSW_GRAPH_ENCODING.md`).
- Direct `SuccinctBM25Index` builder that skips the naive
  intermediate (memory win at large build-time scale).
- Vector quantization for HNSW embeddings — intentionally
  caller-owned via the embedding schema.
- Published release / git push to
  [github.com/triblespace/triblespace-search](https://github.com/triblespace/triblespace-search)
  — awaiting JP's authorization.
