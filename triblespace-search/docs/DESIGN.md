# triblespace-search — design

Two content-addressed index blobs that sit on top of a triblespace
pile: one for BM25-style lexical / associative retrieval, one for
approximate nearest-neighbour search over embeddings. Both follow
the same invariants:

1. **Content-addressed.** Same corpus → same blob hash. Rebuilds
   are free when nothing has changed; same content embedded with
   the same model yields the same blob everywhere in the pile.
2. **Rebuild-and-replace, no mutation.** `build(corpus) -> Handle`
   returns a fresh blob. The caller persists the handle wherever
   it belongs (branch metadata, commit metadata, a plain trible).
   Old index blobs stop being referenced on rebuild; squash
   reclaims them eventually.
3. **Zero-copy views via jerky.** The blob is a self-contained
   byte buffer; a `try_from_blob` produces a view that holds an
   `anybytes::Bytes` backing and answers queries without copying.
4. **Unordered-query shape.** Both indexes expose their query
   primitive as a triblespace constraint, and both follow the
   same rule: doc/handle is the only bound variable, score is
   a fixed parameter (no quantisation bookkeeping in the
   engine path).
     `bm25.matches(?doc, &terms, score_floor: f32)` — multi-
     term BM25 filter. Binds `doc` to documents whose summed
     BM25 across `terms` is `>= score_floor`. Recompute exact
     scores via `idx.score(&doc, &terms)` for ranking.
     `hnsw.similar(?a, ?b, score_floor: f32)` — symmetric
     binary relation: `a` and `b` are
     `Variable<Handle<Blake3, Embedding>>`, `score_floor` is a
     fixed cosine threshold. At least one of `a` / `b` must be
     bound for the engine to walk.
   Callers combine with `and!` / `or!` / filters in the normal
   query engine; ordering is done in Rust after `.collect()`.

## Term is a `Value`

BM25 in `triblespace-search` is not text-specific. Callers supply
terms as 32-byte `Value`s; the library provides a
`hash_tokens(&str) -> Vec<Value>` helper that Blake3-hashes
tokenized words but never forces it on the schema. Downstream uses:

| Term source                       | What this gets you                    |
| :-------------------------------- | :------------------------------------ |
| `hash(word)`                      | Classic text search.                  |
| entity `Id`                       | "Docs mentioning this person."        |
| tag `Id`                          | Tag-weighted search.                  |
| `hash(n-gram)`                    | Phrase search via query rewrite.      |
| fragment `Id`                     | "Docs citing this fragment."          |

The BM25 index is therefore a general `(doc: Id, term: Value, score)`
relation with IDF and length-normalized scoring baked in at build
time.

## `SuccinctBM25Index` — SB25 blob layout

Self-contained blob, zero-copy via `anybytes::Bytes`, bit-packed
via jerky. Schema id: `5A1EF3FFD638B15E3EBEAA1E92660441` (see
`succinct::SuccinctBM25Blob`). The typed `BlobSchema` handle is
the identity — no magic bytes, no version field in the blob.
A breaking format change mints a new schema id.

```
[header              ] 264 B (fixed)
  avg_doc_len             f32    ; for length normalization
  k1                      f32    ; BM25 tuning (default 1.5)
  b                       f32    ; BM25 tuning (default 0.75)
  max_score               f32    ; u16-quantization scale
  n_docs                  u64
  n_terms                 u64
  doc_lens_meta           32 B   ; CompactVectorMetaOnDisk
  postings_doc_idx_meta   32 B   ; CompactVectorMetaOnDisk
  postings_offsets_meta   32 B   ; CompactVectorMetaOnDisk
  postings_scores_meta    32 B   ; CompactVectorMetaOnDisk
  keys_meta               40 B   ; CompressedUniverseMetaOnDisk
  (section_offset, section_len) × 4 = 64 B

[keys                ] variable         ; CompressedUniverse view:
                                        ; 4-byte fragment dictionary
                                        ; (sorted, deduped) + DACs-byte
                                        ; codes, one per unique key.
                                        ; `keys.access(code)` decodes
                                        ; the 32-byte RawValue.
[terms               ] n_terms × 32 B  ; sorted RawValue table
[doc_lens            ] variable         ; jerky CompactVector body
                                        ; width = ceil(log2(max_len + 1))
                                        ; indexed by universe-code order
[postings            ] variable         ; three jerky CompactVectors in
                                        ; one ByteArea:
                                        ;   doc_idx (width log2(n_docs+1),
                                        ;     stores universe codes, not
                                        ;     insertion indexes)
                                        ;   offsets (width log2(total+1))
                                        ;   scores  (width 16, u16-quantized)
```

Every body section starts on an 8-byte boundary; the header's
264-byte length is already a multiple of 8, so no tail padding
is needed. `CompactVector` reinterprets its backing buffer as
`[u64]`, so misalignment would panic at load time.

The four `CompactVectorMetaOnDisk` structs are a
[`zerocopy::IntoBytes`]-deriveable mirror of jerky's
`CompactVectorMeta` (jerky's upstream derives only `FromBytes`).
Four `u64` fields, 32 bytes each, `#[repr(C)]`. The
`CompressedUniverseMetaOnDisk` is the same trick for
`triblespace::core::blob::schemas::succinctarchive::CompressedUniverseMeta`
— five `u64` fields, 40 bytes, `#[repr(C)]`. Static asserts in
`succinct.rs` lock the size and layout equivalence.

Lookup algorithm:
1. Binary-search the term in `terms` (typed
   `View<[[u8; 32]]>` over the canonical bytes — slice's
   `binary_search`) → term index *t*.
2. Read `(offsets[t], offsets[t+1])` from the postings offsets
   CompactVector.
3. For each *i* in that range, read `doc_idx[i]` (a
   `CompressedUniverse` code) from the postings doc_idx
   CompactVector and `score[i]` from the quantized score
   section; decode the external key via
   `keys.access(doc_idx)`.

### What's already compressed (as of the current impl)

- `doc_lens` → bit-packed to `ceil(log2(max_len + 1))` bits.
  At 100k docs with avg_doc_len ≈ 180 and max ≈ 1024, ~10 bits
  instead of 32 — 3.2× savings on that section.
- `postings.doc_idx` → bit-packed to `ceil(log2(n_docs + 1))`.
  At 100k docs, 17 bits instead of 32 — 1.9× savings.
- `postings.offsets` → bit-packed likewise.
- `postings.scores` → u16-quantized via global `max_score`
  scale. Half-bucket error bound = `max_score / 2 × 65535`;
  `score_tolerance` on the index returns the full bucket
  `max_score / 65534` for constraint equality. 2× savings
  vs. f32 on the score section, top-10 preservation verified
  at 1k scale.

### What's still flat (deliberately)

- `terms` — 32 bytes each (Blake3 hash). We tried fragment-
  dictionary compression here (phase 2a) and it **grew** the
  section: Blake3 hashes have maximum entropy, so the 4-byte
  fragment dictionary overhead exceeded any code-length win.
  See `tests/scale_smoke.rs` and the phase 2a revert in git
  history for the actual numbers.

### What keys-side compression bought us

- `keys` is now a `CompressedUniverse` (Phase 2b). Measured via
  `cargo run --release --example blob_sizes_at_scale`:

  | corpus           | keys section vs 32 B flat |
  | :--------------- | :------------------------: |
  | scattered GenIds |        0.74×–0.81×         |
  | 11-byte-prefix   |        0.29×–0.32×         |

  "Scattered" is the pseudo-random `id_from_u64` with 16 trailing
  random bytes (worst-ish case — only the leading 16 zero bytes
  are shared). "Correlated" shares an 11-byte prefix and varies
  only the last 5 — simulates "one session of entity ids minted
  from a shared namespace seed."
- Whole-blob ratio moves too, but modestly: 0.48×→0.42× at 1 k
  docs with correlated keys; ~0.01×–0.02× improvement at 50 k
  because postings dominate the denominator.
- The architectural win is type-level: `keys.access(code)` goes
  through the same universe plumbing as every other `Value`
  table in the stack; range / prefix / membership queries over
  the keys universe compose for free.
### Open compression directions

- **Delta-encoded posting doc_idx** — posting lists are now
  universe-code-sorted (Phase 2b), so consecutive deltas
  compress further via Simple16 / ELF / VByte. Roughly halves
  the `doc_idx` section at Heaps-law corpora. This is the
  next-biggest win to chase — postings dominate the blob.
- **Non-uniform score quantization** — current u16 quantization
  uses a linear global `max_score` scale. A log-space or per-
  term scale would preserve more precision in the high-df
  (common-term) tail at the cost of a bigger header. Only
  worth it if ranking drift bites at larger corpora.
- **Wavelet matrix on the term table** — would let rank/select
  queries hit terms without a linear-compare binary search.
  For identification-only lookups the current
  `View<[[u8; 32]]>` slice-binary-search is competitive; the
  wavelet matrix would unlock range queries over terms (useful
  for n-gram prefix scans).

## `SuccinctHNSWIndex` — SH25 blob layout

Self-contained blob, zero-copy via `anybytes::Bytes`. Schema id:
`27D71A473EF22DA4D916F61810AC5D86` (see
`succinct::SuccinctHNSWBlob`). As with SB25, the typed handle
is the identity — no in-blob magic or version.

```
[header              ] 144 B (fixed)
  dim                     u32
  m                       u16    ; max neighbours on non-zero layers
  m0                      u16    ; max neighbours on layer 0
  max_level               u8
  reserved                u8
  has_entry_point         u8
  reserved                u8
  entry_point             u32
  n_nodes                 u64
  n_layers                u64
  graph_neighbours_meta   32 B   ; CompactVectorMetaOnDisk
  graph_offsets_meta      32 B   ; CompactVectorMetaOnDisk
  (section_offset, section_len) × 3 = 48 B

[handles             ] n_nodes × 32 B          ; Value<Handle<Blake3, Embedding>>
                                               ; — the node IS the handle;
                                               ; no separate doc-key table.
[graph_bytes         ] variable                ; two CompactVectors in one
                                               ; ByteArea:
                                               ;   neighbours (width log2(n+1))
                                               ;   offsets    (width log2(E+1))
```

Schema id: `A96890DE5F85A4F2285C365549B21BC2` (see
`succinct::SuccinctHNSWBlob`; rotated from
`27D71A473EF22DA4D916F61810AC5D86` when the keys section was
dropped).

`graph_bytes` packs neighbour lists across all `(layer, node)`
pairs into a flat CSR: `offsets[L·(n+1) + i]` gives the start of
node *i*'s neighbour list on layer *L* inside `neighbours`. Nodes
absent from layer *L* encode as empty slices — search walks stay
correct because an empty neighbour list is a dead end, and the
search always enters from the top-level entry point.

Query algorithm (standard Malkov-Yashunin search, threshold-gated):
1. Start at `entry_point` on `max_level`.
2. Greedy-descend layer-by-layer down to 1.
3. On layer 0, ef-width beam search; keep every candidate whose
   cosine similarity clears `score_floor`.

The succinct path re-implements the greedy + ef-search against
the bit-packed graph; see
`AttachedSuccinctHNSWIndex::candidates_above` in
`src/succinct.rs`.

### What's already compressed

- Graph `neighbours` → `ceil(log2(n_nodes + 1))` bits per
  neighbour index (17 bits at 100k nodes vs. 32 bits raw).
- Graph `offsets` → `ceil(log2(total_edges + 1))` bits per
  offset, which for `M=16` / `M0=32` averages similar savings.

### What's still flat

- `doc_ids` — 16-byte natural size.
- `vectors` — raw f32. Caller-owned data; compression is the
  caller's decision via their embedding schema choice (the
  crate itself stays agnostic).

### Handle-keyed storage (shipped for both FlatIndex and HNSW)

Both `FlatIndex` and `SuccinctHNSWIndex` store a flat table of
`Value<Handle<Blake3, Embedding>>` (32 B per handle). There is
no separate "doc key" table — the node IS the handle. Callers
who want a book-id → embedding-handle mapping keep it as a
trible attribute they own (`book_embedding` in the examples),
not as shadow data inside the index. Embeddings live in the
pile's blob store, content-addressed, dedup'd across indexes:

```
pile blob store:
  Handle<Embedding> h_a → blob_a  (one copy of vector A)
  Handle<Embedding> h_b → blob_b  (one copy of vector B)

FlatIndex:         [h_a, h_b, h_c, ...]          ← 32 B per entry
SuccinctHNSWIndex: [h_a, h_b, h_c, ...] + graph  ← 32 B per entry + bits
```

The `Embedding` blob schema id is
`EEC5DFDEA2FFCED70850DF83B03CB62B` (minted via `trible genid`).
At query time the walk resolves each handle through
`BlobStoreGet`, and the `BlobCache` wrapper in
`triblespace::core::blob` collapses repeat visits into a
single fetch per view lifetime.

For 100 k × 384-dim MiniLM: the HNSW blob is handles + graph =
~3.2 MiB + bit-packed CSR (a few more MiB); embedding blobs
(~147 MiB total) are dedup'd across every index that references
them.

### Open compression directions

- **2-ring graph encoding** — built and benchmarked in
  `src/ring.rs` + `examples/ring_vs_csr*.rs`. The fixed-
  predicate sub-ring from Arroyuelo et al. *The Ring* (TODS
  2024 §4.4) halves the graph blob vs CSR at every scale
  tested. We *didn't* adopt it as the default because it
  costs ~3× end-to-end query latency on in-memory / warm-
  cache workloads, and at 1B corpus scale the graph is only
  ~4 % of total storage (embeddings dominate). `RingGraph`
  stays as an opt-in primitive for disk-backed or
  branch-metadata-heavy workloads. See
  [`docs/HNSW_GRAPH_ENCODING.md`](HNSW_GRAPH_ENCODING.md)
  for the full measurements and when-to-use-which.
- **Vector quantization** — the biggest lever at scale.
  The caller owns the embedding schema; we could ship
  `EmbeddingI8` / `EmbeddingPQ` alongside `Embedding` and
  let the distance function branch on the schema. At
  dim=384+ the embeddings are 90 %+ of total storage, so
  4–16× quantization shrinks wins far more than any graph
  encoding.

## Query engine integration

Both indexes expose their query as a `triblespace::Constraint`.
Callers load the blob once (cheap — mmap-backed
`anybytes::Bytes`) and produce a constraint by binding the
variables they want:

```rust
let bm25: SuccinctBM25Index = reader.get(bm25_handle)?;
let hnsw: SuccinctHNSWIndex = reader.get(hnsw_handle)?;
let hnsw_view = hnsw.attach(&reader);

let rows: Vec<(Id,)> = find!(
    (doc: Id),
    and!(
        pattern!(&kb, [{ ?doc @ wiki::content: _ }]),
        bm25.matches(doc, &terms, 0.0),
    ),
)
.collect();
```

BM25 binds `doc` only — `matches(doc, &terms, score_floor)` is
a single-variable filter; ranking happens in Rust via
`idx.score(&doc, terms)` after `.collect()`. HNSW similarity
is a binary `similar(a, b, score_floor)` relation over
`Value<Handle<Blake3, Embedding>>` variables (see
`docs/QUERY_ENGINE_INTEGRATION.md`). Ordering is operational —
callers collect the iterator and slice.

## What lives where

| Concern                       | Crate                   |
| :---------------------------- | :---------------------- |
| `Value`, `Id`, `TribleSet`    | triblespace             |
| Blob byte buffers (mmap)      | anybytes                |
| Succinct primitives           | jerky                   |
| BlobSchema + constraints      | **triblespace-search**  |
| Tokenizers (opt-in helpers)   | **triblespace-search**  |
| Caller-supplied embeddings    | downstream              |

`triblespace-search` does not depend on any embedding library.
Callers bring their own embeddings (local MiniLM via fastembed,
API-based Voyage/OpenAI, or anything that produces `f32` vectors
of a fixed dimensionality) and insert them into the pile under
an `Embedding<const D: usize>` schema they control.

## Non-goals (v1)

- Mutable updates. Rebuild is the only update path.
- Distributed/sharded indexes. Single-node first; sharding lives
  above the index API if/when it matters.
- Language-aware tokenization. `hash_tokens` is intentionally
  minimal; callers with real NLP needs tokenize themselves.
- Linear score combinations across BM25 + HNSW (hybrid search).
  Caller composes the boolean combination through `and!` /
  `or!` in `find!` (see `examples/hybrid_search.rs`); if they
  want to rank on a weighted sum of scores, they do so in Rust
  after `.collect()`.

## Worked example: 100 000 wiki fragments

Sizing exercise for the canonical downstream: indexing a Liora
pile of ≈ 100 k typst wiki fragments, average ≈ 180 words each
(≈ 300 raw tokens with punctuation). Numbers are back-of-envelope
for the *naive* (current) layout — the jerky succinct pass will
shrink the term-heavy sections.

### BM25 — size estimate

Assume after `hash_tokens`:
- `n_docs = 100 000`
- `avg_doc_len ≈ 180` unique tokens per doc after trim/dedup
- distinct terms across corpus `n_terms ≈ 300 000` (Heaps' law
  with β ≈ 0.5, k ≈ 30 for English-ish text)
- total postings `≈ 100 000 × 180 = 18 000 000` entries

Two columns: a theoretical "naive flat-array" layout (the
pre-jerky baseline this crate started from — reported by
[`BM25Index::byte_size`], no actual serializer ships) and the
landed SB25 format (`SuccinctBM25Index::to_bytes`) with
bit-packing + score quantization.

| Section            | Per-entry | Count      | Naive bytes | SB25 bytes  |
| :----------------- | --------: | ---------: | ----------: | ----------: |
| header             | —         | —          |       20 B  |     264 B   |
| keys               |    32 B   | 100 000    |   3.2 MiB   | ~1.5–3.2 MiB|
| doc_lens           |     4 B   | 100 000    |   0.4 MiB   | ~0.12 MiB   |
| terms (sorted)     |    32 B   | 300 000    |   9.6 MiB   |  9.6 MiB    |
| postings_offsets   |     4 B   | 300 001    |   1.2 MiB   | ~0.6 MiB    |
| postings.doc_idx   |     4 B   | 18 000 000 |    72 MiB   | ~38 MiB     |
| postings.score     |     4 B   | 18 000 000 |    72 MiB   |    36 MiB   |
| **Total**          |           |            | **~159 MiB**| **~86 MiB** |

Every row computed the same way: the bit-packed sections use
`ceil(log2(n + 1))` bits per entry (doc_idx → 17 bits ≈ 2.12 B;
doc_lens at max ≈ 1024 → 10 bits ≈ 1.25 B; offsets at 18M max →
25 bits ≈ 3.1 B), and u16-quantized scores drop from 4 B to 2 B.

The `keys` range covers the fragment-dictionary compression
spread: near-worst-case (random 32-byte values, no shared 4-byte
fragments) ≈ raw 3.2 MiB plus a small DACs overhead; typical
GenId-keyed corpora with 16 bytes of zero padding and structured
trible bytes compress toward ~1.5 MiB. Neither end moves the
blob total noticeably — keys are ~2 % of the 86 MiB.

The **postings dominate** at 85 %+ of either blob. SB25's bit-
packed `doc_idx` plus u16 scores halves that section — the rest
of the footprint (keys, terms, docs_lens) is already as small as
the data allows without additional structure.

Term table is the second-largest chunk (9.6 MiB of 32-byte
Blake3 hashes). Phase 2a tried fragment-dictionary compression
here and it made the section **bigger** — maximum-entropy hashes
have no shared 4-byte fragments for the dictionary to exploit.
Left uncompressed.

### BM25 — build time

Build is O(total postings) with hashmap bookkeeping: `18 M`
insertions into the `HashMap<RawValue, HashMap<u32, u32>>` tf
table, then a sort over 300 k term hashes (32-byte compare).
On current laptop hardware:
- Hash-tokenize 100 k fragments × 180 tokens ≈ 18 M Blake3 hashes.
  Blake3 is ~3 GB/s on short inputs → ~0.5 s.
- Hashmap inserts: ~100 ns each × 18 M ≈ 1.8 s.
- Term sort: 300 k × log₂(300 k) × 32-byte compare ≈ 50 ms.
- Score computation: 18 M FMA-ish float ops ≈ 50 ms.

So **~3 s single-threaded** for the full corpus.
`BM25Builder::build_with_threads(n)` shards docs across `n`
scoped threads (std::thread::scope, no rayon dep) and merges
per-shard tf maps at the end. Observed speedups at 4 threads
on a laptop: ~1.2× at 10 k docs, ~1.3× at 50 k — the merge
cost stays serial and caps the win. Byte-identical output vs.
single-threaded. A term-partitioned variant would push further
but needs a routing hash per insert; filed as future work when
build-time actually bites.

### BM25 — query latency

`cargo run --release --example query_latency` on current laptop
hardware (10 k / 50 k docs with Zipf-ish vocab):

| Corpus              | Path   | p50     | avg     | p99     |
| :------------------ | :----- | ------: | ------: | ------: |
| 10 k × 64 tokens    | naive  |  125 ns |  162 ns |  459 ns |
| 10 k × 64 tokens    | SB25   |  875 ns | 1.06 µs | 4.04 µs |
| 50 k × 96 tokens    | naive  |  292 ns |  417 ns | 2.29 µs |
| 50 k × 96 tokens    | SB25   | 2.00 µs | 2.82 µs | 6.21 µs |

Single-term queries stay comfortably under 3 µs on the succinct
path even at 50 k docs — the original design estimate was a
generous upper bound. The naive path is ~6-7× faster than SB25
(flat `Vec` read + pre-baked `f32` vs. bit-unpacking a
`CompactVector` + dequantizing a u16 score); SB25 trades that
latency for ~2× smaller blobs on disk.

3-term `query_multi` (OR of independent posting lists):
~207 µs p50 at 50 k docs, dominated by the aggregation
`HashMap<Id, f32>`. For latency-critical multi-term queries an
`and!` via the engine is cheaper — merge-join size is
`min(|postings|)`, not `sum(|postings|)`.

### HNSW — size estimate

At `n = 100 000`, `dim = 384` (MiniLM), `M = 16`, M0 = 32.
Embeddings are not in the HNSW blob — they live in the pile's
blob store, content-addressed, and shared across every index
that references them. The HNSW blob only carries the handles
table and the graph:

- `handles`: 100 k × 32 B = **3.2 MiB** (one
  `Value<Handle<Blake3, Embedding>>` per node, the sole
  per-node table)
- graph `neighbours`: ~1 M directed edges (average `M`
  neighbours per node plus layer-0 fill-in with `M0 = 32`),
  each packed at `ceil(log2(n + 1)) = 17` bits ≈ 2.1 MiB
- graph `offsets`: `(layers + 1) × n` entries at
  `ceil(log2(edges + 1)) = 20` bits; layers stay ~4–5 by
  design (`log_M(100k) ≈ 4`), so roughly 500 k entries ≈
  1.25 MiB
- SH25 header: 128 B (negligible)
- **Total HNSW blob ~6.5 MiB.**

Separately, in the pile's blob store:
- `Embedding` blobs: 100 k × 384 × 4 B = **147 MiB** (caller-
  owned, dedup'd — two indexes over the same vectors share
  the bytes)

The handle-indirected design moves embedding compression out
of this crate's surface: `Embedding` is agnostic to the on-
disk encoding, so callers who care about footprint swap in
`EmbeddingI8` / `EmbeddingPQ` schemas at their level. The
crate's own succinct pass targets the graph — which at this
scale is already ~4 % of the total corpus footprint, so
there's no transformative graph win to chase.

### HNSW — query latency

`query_latency` example on 5 k / 10 k × 32-dim corpora, probes
sampled from the indexed handles, threshold `cos ≥ 0.5`,
`ef_search = 50`:

| Corpus          | Path  | p50      | avg      | p99      |
| :-------------- | :---- | -------: | -------: | -------: |
| 5 k × 32        | naive | 190 µs   | 191 µs   | 215 µs   |
| 5 k × 32        | SH25  | 190 µs   | 191 µs   | 222 µs   |
| 10 k × 32       | naive | 226 µs   | 228 µs   | 273 µs   |
| 10 k × 32       | SH25  | 220 µs   | 221 µs   | 248 µs   |

SH25 tracks naive within noise — both paths fetch every
visited embedding through the same `BlobCache<MemoryBlobStore,
Embedding>` and compute cosine against a
contiguous `&[f32]` view, so the graph-access difference
(pointer hop vs. bit-unpack) is swamped by the O(ef_search ×
dim) distance-eval work. Threshold walks visit more nodes
than the old top-k shape did (no early exit once `k` hits),
which accounts for the absolute-number jump versus earlier
measurements.

### Takeaways

- Naive BM25 blob is ~1.5 KiB per doc — already shippable as a
  scaffold; SB25 halves that.
- Postings are the biggest lever; bit-packing + u16 scores
  already claimed it. The next step (delta-encoded `doc_idx`,
  wavelet-matrix term table) is incremental, not transformative.
- For HNSW, the interesting compression sits in *caller-owned*
  embedding bytes — this crate's pass is about graph compactness
  and graph-walk speed, not bulk size.
- At these scales a single-node mmap-backed blob is fine; the
  "distributed indexes" non-goal holds even at 1 M docs.
