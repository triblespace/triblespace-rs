# Inventory

## Potential Removals
- None at the moment.

## 0.7.0 Release Preparation
- **Delta helpers.**
  - Design a helper that produces delta `TribleSet`s for `pattern_changes!` and
    drafts an API signature for review.
  - Implement the helper plus unit tests that demonstrate incremental update
    workflows and guard against regressions.
  - Document the helper in the book or API docs with a migration note for users
    maintaining custom delta code.
- **Documentation polish.**
  - Draft advanced query examples that compose multiple `attributes!` modules
    and slot them into the book structure.
  - Extract deep reference content from the API docs (`value`, `blob`, `repo`,
    and trible structure discussions) into dedicated book chapters.
  - Author the requested FAQ chapter and cross-link it from the landing page and
    changelog for discoverability.
- **PATCH performance notes.**
  - Stand up a repeatable benchmark suite covering the iterator and
    `with_sorted_dedup` improvements.
  - Summarise empirical findings alongside complexity notes in either the book
    or changelog.
  - Capture any uncovered hotspots or tuning ideas back into this inventory for
    future releases.

## Query engine documentation follow-ups
- `triblespace-core/src/query/residual/` is still tracked (`delta.rs`,
  `materialize.rs`, `positive_hedge_credit.rs`, `set_admit.rs` — ~640 KB) but
  no `mod residual;` declaration reaches it anywhere in the crate. Orphaned by
  the residual-engine deletion; delete it or re-attach it deliberately.
- The `ProjectionKey` type alias in `triblespace-core/src/query.rs` is dead —
  it keyed the terminal projection claim table, which no longer exists.
- The `find!` macro's doc comment in `triblespace-core/src/query.rs` still
  documents relational SET semantics, raw-head claiming before conversion, and
  the "at most one `()`" rule for the unit head. The engine is a bag of
  complete bindings (see the F8 fixture in `tribleset-bench`), so the doc
  comment contradicts both the implementation and the rewritten book chapters.
  The `Constraint` trait's own doc table likewise lists five methods for a
  seven-method trait (`propose_chunk` and `influence` are missing).
- The `[Unreleased]` section of `CHANGELOG.md` still carries entries describing
  the deleted residual/typed-Program engine (residual compiler policy, typed
  Program pagers, `OrderKeyMode`, RPQ scheduling) as if they shipped. They
  should be reconciled before the next release notes are cut.
- A book chapter on the `triblespace-paths` closure index is owed once that
  crate's surface stabilises; the interim guidance lives in
  `book/src/query-language.md#recursive-traversal`.

## Desired Functionality
- Reconcile the residual branch's workspace-wide rustfmt baseline (or pin the
  intended formatter toolchain): `cargo fmt --all` currently rewrites many
  unrelated files, obscuring focused query-engine diffs.
- Provide additional examples showcasing advanced queries and repository usage.
- Helper to derive delta `TribleSet`s for `pattern_changes!` so callers don't
  have to compute them manually.
- Add an exporter for the lossless JSON schema so archived JSON can be
  reconstructed (including field ordering).
- Add a diagnosis tool that reports attributes missing `name`, `value_encoding`,
  or `value_formatter` metadata so strict renderers can explain omissions.
- Explore replacing `CommitSelector` ranges with a set-based API
  built on commit reachability. The goal is to mirror git's revision
  selection semantics (similar to `rev-list` or `rev-parse`).
  Combinators like `union`, `intersection` and `difference` should let
  callers express queries such as "A minus B" or "ancestors of A
  intersect B". Commit sets themselves would be formed by primitives
  like `ancestors(<commit>)` and `descendants(<commit>)` so selectors
  map directly to the commit graph.
- Add tests that cover `CommitSelector` and `Workspace::checkout` behavior when
  a branch has no head commit.
- Generate `attributes!` modules from a `TribleSet` description so tooling can
  derive them programmatically. Rewriting `pattern!` as a procedural
  macro will be the first step toward this automation.
- Benchmark PATCH performance across typical workloads.
- Investigate the theoretical complexity of PATCH operations.
- Measure practical space usage for PATCH with varying dataset sizes.
- Explore hash-prefix-partitioned Pile bootstrap PATCH construction: keep all
  duplicate candidates for a key in one ordered worker, retain serial pin LWW,
  and merge only disjoint key ranges so value-insensitive PATCH union cannot
  alter first-valid duplicate selection.
- Extend PATCH to associate values with keys, turning it into a map structure.
- Expose value-aware PATCH iterators and lookup helpers so callers can access
  stored payloads.
- Benchmark recursive `ByteTable` displacement planner versus the greedy random insert to measure fill rate and performance across intermediate table sizes.
- Explore converting the recursive `ByteTable` planner into an iterative search to reduce stack usage.
- Implement a garbage collection mechanism that scans branch and commit
  archives without fully deserialising them to find reachable blob handles.
  Anything not discovered this way can be forgotten by the underlying store.
- Generalise the declarative key description utilities to other key types so
  segment layouts and orderings can be defined once and generated automatically.
- Provide a macro to declare key layouts that emits segmentation and
  ordering implementations for PATCH at compile time.
- Expose segment iterators on PATCH using `KeySchema`'s segment permutation instead of raw key ranges.
- Consolidate pile header size constants to avoid repeated magic numbers.
- Add an explicit `Pile::put` guard/error for oversized single-record appends
  (e.g. platform `writev` limits) so failures are deterministic and actionable.

## Formal Verification
### Invariant Catalogue
- Translate the `book/src/formal-verification.md` matrix into individual GitHub
  issues, each covering one subsystem (TribleSet, PATCH, values, queries,
  repository, storage primitives).
- Document how each invariant maps to existing modules so new contributors can
  locate the relevant code without spelunking.

### Harness Work
- Make the public `triblespace-paths` product-oracle harness tractable for
  full CBMC verification. `cargo kani -q --package=triblespace-paths --harness
  path_index_matches_two_vertex_product_oracle --only-codegen` succeeds, but a
  32-subgraph solve was capped after 347 seconds without a verdict, and the
  original 256-graph symbolic family was capped after ten minutes while using
  roughly 16 GiB. The same 256 cases pass instantly as a native exhaustive
  test. The fixed closure carrier has only four product nodes; the dominant
  formula comes from the public `Automaton`/`PathSummary` path through `Vec`
  allocation and `BTreeSet` canonicalization/destruction. Investigate a sound
  proof-only abstraction for those already-tested canonical containers, or a
  separately callable fixed-carrier closure kernel, before increasing bounds.
- Generalise the `triblespace-paths` product-oracle rung beyond its exhaustive
  two-vertex, fixed two-state automaton: first add a non-nullable automaton rung,
  then bound symbolic transition tables without making private closure
  internals part of the verification surface.
- Build shared bounded-data generators for Kani harnesses (tribles, PATCH
  entries, commit DAGs) and publish them under `proofs/util.rs`.
- Add `proofs/tribleset_harness.rs` validating ordering-preserving union,
  intersection, difference, and iterator round-trips.
- Add `proofs/patch_harness.rs` with ByteTable checks proving `plan_insert`
  respects `MAX_RETRIES`, `table_insert` hands growth entries back to
  `Branch::modify_child`, and `table_grow` preserves every occupant.
- Extend `proofs/value_harness.rs` with schema-aware helpers ensuring
  `TryFromInline` conversions reject truncated buffers.
- Expand `proofs/commit_harness.rs` with bounded commit DAG generators that
  assert append-only pile semantics.

### Tooling & Execution
- Integrate `cargo miri test` into `scripts/preflight.sh` with appropriate
  guards for unsupported harnesses.
- Stand up a `cargo fuzz` workspace covering PATCH encoding/decoding, query
  planning, and repository sync flows; publish nightly cadence expectations in
  the roadmap.
- Record deterministic simulation scenarios (conflict resolution, garbage
  collection, remote sync) that double as regression tests.

## Additional Built-in Schemas
The existing collection of schemas covers the basics like strings, large
integers and archives.  The following ideas could broaden what can be stored
without custom extensions:

### Inline schemas
- `Uuid` for RFC&nbsp;4122 identifiers.
- `Ipv4Addr` and `Ipv6Addr` to store network addresses.  IPv6 could dedicate
  spare bits to a port or service code.
- `SocketAddr` representing an IP address and port in one value.
- `MacAddr` for layer‑2 hardware addresses.
- `Duration` for relative time spans.
- `GeoPoint` with latitude and longitude stored as two 64‑bit floats.
- `RgbaColor` packing four 8‑bit channels into one value.
- `BigDecimal` for high‑precision numbers up to 256 bits.

### Blob encodings
- `Json`, `Cbor` and `Yaml` for structured data interchange.
- `Csv` for comma‑separated tables.
- `Protobuf` or `MessagePack` for compact typed messages.
- `Parquet` and `Arrow` for columnar analytics workloads.
- `Lance` for memory-mapped columnar datasets.
- `CompressedBlob` wrapping arbitrary content with deflate or zip compression.
- `WasmModule` for executable WebAssembly.
- `OnnxModel` or `Safetensors` for neural networks.
- `HnswIndex` for vector search structures.
- `TantivyIndex` capturing a full-text search corpus.
- `Url` for web links and other IRIs; best stored as a blob due to the value
  size limit.
- `Html` or `Xml` for markup documents.
- `Markdown` for portable text.
- `Svg` for vector graphics.
- `Png` and `Jpeg` images.
- `Pdf` for print‑ready documents.

Formats with solid memory-mapping support in the Rust ecosystem should be
prioritized for efficient zero-copy access.

## Documentation
- Add diagrams or pseudocode to the Atreides Join chapter illustrating variable selection and search.
- Move the "Portability & Common Formats" overview from `src/inline.rs` into a
  dedicated chapter of the book.
- Migrate the blob module introduction in `src/blob.rs` so the crate docs focus
  on API details.
- Extract the repository design discussion and Git parallels from `src/repo.rs`
  into the book.
- Split out the lengthy explanation of trible structure from `src/trible.rs`
  and consolidate it with the deep dive chapter.
- Add a FAQ chapter to the book summarising common questions.

## Discovered Issues
- Add an executor-local shadow observer at the residual action-task boundary.
  It should quote critical-path and total service cost for the exact
  `(action, bound schema, batch geometry)` without giving planning-only Ready
  or Candidate states a fabricated backend quote. Keep observation opt-in
  until its clock/counter cost is measured, then compare an unsplit parent
  task with concrete child tasks using confidence and reconvergence loss
  rather than a global hardware cutoff.
- Publish the checked Rank9 sidecar seam as a new Jerky crate version, then
  replace the exact git-revision pins in `triblespace-core` and
  `triblespace-search` before the next crates.io release. The git pin is an
  intentional integration bridge, not the final publishable dependency.
- Index-home kind IDs currently identify the implementation but not the full
  index recipe. Derive or persist recipe identity for configuration such as a
  BM25 content attribute/tokenizer version and HNSW dimensions/metric so
  incompatible segment families cannot share one manifest or coverage
  certificate.
- Define archive-message semantics when one entity carries multiple content
  handles. BM25 preserves the union of their term presence, while result
  materialisation currently selects one matching body; either make the schema
  cardinality explicit or make resolution deterministic and test it.
- Make `IndexKind::build` fallible (or split out a fallible resolver-backed
  build surface). BM25/HNSW kinds cannot currently report an unreadable source
  handle through the trait; archive indexing prevalidates LongString content,
  but generic callers can otherwise build a segment that silently omits it.
- Extend commit-native index-home testing with an interrupted bootstrap over a
  true merge DAG (multi-tip frontier plus CAS conflict), an actual commit above
  the physical shard threshold proving all shards share one atomic coverage
  advance, and explicit backward/divergent branch-head rejection.
- Property-test BM25 max-union compaction across randomized segment
  permutations, repeated multi-level FANOUT merges, and high term frequencies
  near score-quantization saturation.
- The optional CubeCL succinct-merge backend's per-level block-prefix scan is
  still one serial device thread. Packed CPU reduced the measured WGPU gain to
  5–8% on large Apple Metal tiers; investigate a hierarchical device scan and
  rotation batching before considering GPU acceleration for default archive
  maintenance. Keep the summed-input crossover hardware-calibrated.
- Yard collection currently evicts blobs from per-generation live PATCH sets
  while leaving the append-only Pile records in place. Add a future physical
  compaction/rewrite path when Yard needs to reclaim disk space, preserving
  live readers while replacing generation files.
- Define retirement or service-selection semantics for asserted wants that
  exceed a finite `YardConfig::want_budget`. Grow-only wants deliberately
  survive satisfaction and eviction, so collection can evict an unbudgeted
  blob and reconciliation can fetch it again indefinitely. The current API has
  neither typed physical forgetting for selected wants nor a policy for which
  authored wants the reconciler should decline to service.
- The packed device confirm path assumes `UNIT_POS_PLANE` relates linearly to
  the cube-local invocation index — condition (c) on
  `membership_confirm_ballot_kernel`. It is true on Metal and CUDA and is what
  makes ballot bit `L` the verdict of the lane at `plane_base + L`, but the
  WGSL subgroups extension leaves the invocation-to-subgroup mapping
  implementation-defined and a violation shows up only as wrong query answers.
  A standalone "pack N predicate bits with a ballot and compare against a CPU
  pack" kernel, run over `n x bit_offset` grids, would turn the assumption into
  a measurement on each adapter we ship on; it is also the test the
  shared-memory-atomic alternative would need.
- The packed confirm kernels hardcode ballot component 0 and are gated on
  `plane_size_min == plane_size_max == 32` (`require_plane_packing`). Widening
  to 64-lane planes needs a *dynamic* index into `Vector<u32, Const<4>>`
  (`ballot[UNIT_POS_PLANE / 32]`), which cubecl 0.10 has no in-tree usage of
  and which naga's MSL backend would defeat anyway — it writes components
  1..3 as literal zeros. Only worth doing if a 64-wide target enters scope.
- The confirm round trip is dominated by fixed cost, not by the verdict
  buffer: three fresh device allocations per membership confirm, six per range
  confirm, and one blocking readback. Packing the verdicts 32x shrinks the part
  that was already ~10% of the trip. The order-of-magnitude move is keeping the
  region's liveness resident on the device across confirms — at which point two
  confirms over adjacent regions of the same buffer really do collide on the
  shared edge word and the merge has to become `atomicAnd`, which kill-only
  makes idempotent and order-free.
- `ProposalBuffer::retain_region` moves each survivor's liveness bit down one
  index at a time. It is correct (writes only trail reads) but is a per-entry
  read-modify-write where the word-per-candidate layout had a slot copy; it is
  the one compaction path packing made *worse*, and it runs once per
  `UnionConstraint::propose` variant that is satisfied on only some rows. If a
  union-heavy profile ever shows it, the word-aligned bulk case (`base % 32 ==
  0` and a run of survivors) can be lifted to whole-word shifts.
