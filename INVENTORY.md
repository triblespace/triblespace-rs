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
    (including the regular path walkthrough) and slot them into the book
    structure.
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

## Desired Functionality
- Provide additional examples showcasing advanced queries and repository usage.
- Include a regular path query example that combines attributes from multiple
  `attributes!` modules in the book.
- Helper to derive delta `TribleSet`s for `pattern_changes!` so callers don't
  have to compute them manually.
- Add an exporter for the lossless JSON schema so archived JSON can be
  reconstructed (including field ordering).
- Add a diagnosis tool that reports attributes missing `shortname`,
  `value_schema`, or `value_formatter` metadata so strict renderers can explain
  omissions.
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

## Formal Verification
### Invariant Catalogue
- Translate the `book/src/formal-verification.md` matrix into individual GitHub
  issues, each covering one subsystem (TribleSet, PATCH, values, queries,
  repository, storage primitives).
- Document how each invariant maps to existing modules so new contributors can
  locate the relevant code without spelunking.

### Harness Work
- Build shared bounded-data generators for Kani harnesses (tribles, PATCH
  entries, commit DAGs) and publish them under `proofs/util.rs`.
- Add `proofs/tribleset_harness.rs` validating ordering-preserving union,
  intersection, difference, and iterator round-trips.
- Add `proofs/patch_harness.rs` with ByteTable checks proving `plan_insert`
  respects `MAX_RETRIES`, `table_insert` hands growth entries back to
  `Branch::modify_child`, and `table_grow` preserves every occupant.
- Extend `proofs/value_harness.rs` with schema-aware helpers ensuring
  `TryFromValue` conversions reject truncated buffers.
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

### Value schemas
- `Uuid` for RFC&nbsp;4122 identifiers.
- `Ipv4Addr` and `Ipv6Addr` to store network addresses.  IPv6 could dedicate
  spare bits to a port or service code.
- `SocketAddr` representing an IP address and port in one value.
- `MacAddr` for layer‑2 hardware addresses.
- `Duration` for relative time spans.
- `GeoPoint` with latitude and longitude stored as two 64‑bit floats.
- `RgbaColor` packing four 8‑bit channels into one value.
- `BigDecimal` for high‑precision numbers up to 256 bits.

### Blob schemas
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
- Move the "Portability & Common Formats" overview from `src/value.rs` into a
  dedicated chapter of the book.
- Migrate the blob module introduction in `src/blob.rs` so the crate docs focus
  on API details.
- Extract the repository design discussion and Git parallels from `src/repo.rs`
  into the book.
- Split out the lengthy explanation of trible structure from `src/trible.rs`
  and consolidate it with the deep dive chapter.
- Add a FAQ chapter to the book summarising common questions.

## Discovered Issues
- No open issues recorded yet.
