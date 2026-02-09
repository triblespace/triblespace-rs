# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- `Repository::create_branch` now mints branch ids with `genid` (high-entropy random ids) instead of `ufoid` (time-prefixed ids).

## [0.11.0] - 2026-02-08
### Added
- Branch tombstone records in the pile format for explicit branch deletion.

### Changed
- `BranchStore::update` now takes `new: Option<Handle<..>>`; `None` deletes the branch.
- `Pile` applies tombstones by removing branch heads from its in-memory branch index.
- `ObjectStoreRemote` represents tombstones as empty branch objects (size=0) and filters them out of `branches()`.

## [0.10.0] - 2026-02-07
### Added
- Attribute usage annotations with `metadata::attribute`,
  `metadata::source_module`, and `KIND_ATTRIBUTE_USAGE` for capturing
  contextual names/descriptions.

### Changed
- Clarified `metadata::name` and `metadata::description` as general-purpose
  entity naming/description attributes in docs and metadata comments.
- `Attribute::describe` now emits usage annotations when available, and the
  `attributes!` macro attaches contextual usage metadata (name/description/
  source) to declared attributes.
- Attribute usage ids are now anchored on the attribute id + module path only,
  so renames and file/line shifts do not churn usage identities.
- JSON importers validate UTF-8 strings via `View<str>` while reusing the
  parsed bytes.

## [0.9.0] - 2026-02-03
### Added
- Lossless JSON importer that preserves structure and ordering with explicit
  node/entry entities and content-addressed ids.
- `FileBytes` blob schema for explicit file-backed byte payloads.

### Changed
- Removed the serde-based and non-deterministic JSON importers; the remaining
  deterministic importer is now `JsonObjectImporter`.
- Renamed JSON importers for clarity: `JsonImporter` -> `JsonObjectImporter`,
  `LosslessWinnowJsonImporter` -> `JsonTreeImporter`, and `json_lossless` ->
  `json_tree`.

### Fixed
- Added the missing `Value` import in the lossless JSON importer.

## [0.8.0] - 2026-01-22
### Added
- `TribleSetFingerprint` plus `TribleSet::fingerprint` for fast, in-process
  cache keys that match `TribleSet` equality.
- `Workspace::commit` now accepts optional commit metadata, plus
  `Workspace::checkout_metadata` and `Workspace::checkout_with_metadata` for
  reading commit metadata `TribleSet`s. Supplying commit metadata does not
  modify the workspace default.
- `Repository::set_default_metadata` and `Workspace::set_default_metadata`
  for configuring default commit metadata handles, plus
  `Repository::pull_with_metadata` for per-workspace overrides.
- `Repository::storage` and `Repository::storage_mut` for direct access to the
  underlying storage backend.
### Changed
- Renamed `WasmFormatterLimits` to `WasmLimits`.
- Commits can now carry an optional `metadata` handle alongside `content`.
- `repo::commit::commit_metadata` now takes an optional metadata handle instead
  of a metadata blob.
- `CommitSelector` ranges now treat branches without a head commit as empty
  history, so `Workspace::checkout` returns an empty `TribleSet` instead of an
  error when no commits exist.
- JSON importers now include schema metadata in their emitted metadata sets so
  value formatter lookups can succeed.

## [0.7.0] - 2026-01-18
### Changed
- Updated the README quote to Joe Armstrong’s talk “The Mess We’re In.”
- `Metadata` and `ConstMetadata` now use a shared `id` method as the canonical
  schema identifier, eliminating the former `metadata_id` accessors.
- `Metadata::describe` and `ConstMetadata::describe` are now fallible so blob
  write errors can be propagated instead of silently ignored.
- `ValueSchema` inherits its identifier and default description behavior from
  `ConstMetadata`, removing duplicate `id`, `metadata_id`, and `describe`
  methods from the schema trait itself.
- Hash protocol metadata now emits the protocol name alongside the identifier
  so descriptions include the declared `NAME` for each digest.
- Replaced the `SchemaMetadata` helper with direct `ConstMetadata` impls on
  value schemas so static metadata stays in sync with runtime metadata roots.
- Removed explicit blob schema hooks from value schemas and attribute metadata,
  relying on metadata identifiers instead of nested blob schema entries.
- Updated schema documentation to reflect metadata-driven identifiers and
  examples that call `ConstMetadata::id()`.
- `Handle` value schemas now forward metadata from their hash protocol and blob
  schema components so composite schema descriptions stay discoverable.
- Removed the WASM module byte-size limit checks from module compilation and
  formatter loading; callers can enforce limits by checking blob sizes before
  compilation.
- `triblespace_core::wasm::shared_engine` is no longer part of the public API;
  module compilation uses an internal, lazy-initialized engine.
- Hash/handle WASM value formatters now always use the generic hex formatter,
  instead of special-casing specific hash protocols.
- Hash/handle WASM formatter output now prefixes `hash:` before the hex digest.
- Inlined schema-level value formatter metadata emission, removing the
  `wasm_formatters` helper module.
- Metadata describe helpers now bind schema IDs once and inline blob puts when
  emitting tribles.
- `entity!` now accepts `ExclusiveId` values in addition to `&ExclusiveId`
  references.
- Renamed `ExclusiveId::as_transmute_force` to `ExclusiveId::force_ref`.
- WASM value formatter limits are now supplied per formatting call (with a
  default helper), and the eager formatter loader no longer captures limits at
  construction time.
- `WasmModuleResolver` has been removed in favor of the generic `BlobCache`.
- `WasmValueFormatterResolver` has been removed in favor of direct
  `metadata::value_formatter` lookups with `BlobCache`.
- `load_wasm_value_formatters` has been removed in favor of `BlobCache` and
  `metadata::value_formatter` lookups.
- `#[value_formatter]` can override the generated WASM byte constant name and
  visibility via `const_wasm = NAME` and `vis(...)` arguments.
- Attribute identifiers derived from hashed names now use the rightmost 16 bytes
  of the Blake3 digest to stay consistent with the ID-to-value layout.
- Consolidated JSON import into a single deterministic
  `import::json::JsonObjectImporter` with fixed primitive mappings and optional salt
  support, replacing the prior nondeterministic importer and configurable
  encoder callbacks.
### Added
- Guidance on how `ExclusiveId` ownership narrows safe absence checks while
  keeping queries monotonic across collaborators in the incremental queries
  chapter of the book.
- `metadata::KIND_VALUE_SCHEMA` and `metadata::KIND_BLOB_SCHEMA` tags, now
  emitted by built-in schema metadata for discovery.
- `metadata::description`, a LongString-backed attribute for schema
  documentation, and `metadata::name`/`metadata::description` emission for
  built-in value and blob schemas.
- `metadata::Metadata` trait for emitting self-describing `TribleSet` and
  `MemoryBlobStore` pairs, enabling attributes and schemas to publish
  documentation metadata recursively.
- `TryToValue` implementations that convert `serde_json::Number` directly into
  the `F256` schema so JSON import code can call `.to_value()` instead of
  hand-packing high-precision floats.
- Criterion benchmark covering deterministic JSON import performance using the
  serde-rs/json-benchmark fixtures.
- `GenId` value schema conversions for `uuid::Uuid`, including fallible packing and support for nil UUID values
  and unpacking helpers that enforce the non-nil invariant.
- Bundled the `canada.json`, `citm_catalog.json`, and `twitter.json` datasets to
  keep the JSON import benchmark self-contained.
- `import::json::JsonObjectImporter` for deterministic JSON imports that map strings
  to `Handle<Blake3, LongString>`, numbers to `F256`, booleans to `Boolean`,
  and nested objects to `GenId` links, hashing attribute/value pairs (with an
  optional 32-byte salt) to derive stable entity ids, while streaming blobs into
  a caller-provided store and exposing data/metadata separately.
- `valueschemas::Boolean` for encoding `false` as all-zero bytes and `true` as
  all ones, providing an unambiguous target for JSON boolean importers.
- `RangeU128` and `RangeInclusiveU128` value schemas for encoding pairs of
  packed `u128` values, enabling compact storage of start/end markers such as
  source ranges.
- `LineLocation` value schema for storing explicit `(line, column)` start and
  end coordinates without manual packing, now used by the macro metadata
  instrumentation when recording invocation spans.
- `wasm` feature flag that bundles WebAssembly value formatters for the built-in
  value schemas and attaches them via `metadata::value_formatter` when emitting
  schema metadata.
- `#[value_formatter]` proc macro support in `triblespace-core-macros`, enabling
  the core crate to compile and embed formatter modules without introducing a
  dependency cycle.
- `triblespace-macros` crate wrapping the procedural macros and query helpers
  to record invocation metadata in an optional repository configured via the
  `TRIBLESPACE_METADATA_PILE` and `TRIBLESPACE_METADATA_BRANCH` environment
  variables.
- `TRIBLESPACE_METADATA_SIGNING_KEY` environment variable for configuring the
  signing key used when committing metadata; instrumentation skips emission when
  the value is unset or invalid.
- `Id::from_hex` helper for parsing hexadecimal identifiers, now reused by the
  macro metadata instrumentation when decoding branch IDs.
- Attribute definition metadata emitted alongside `attributes!` expansions,
  recording attribute identifiers, names, invocation IDs, and the declared
  schema type tokens for downstream analysis tools.
- Runtime helper `Attribute::from_name` for deriving deterministic attribute IDs
  from dynamic field names using schema metadata and hashed field handles.
- Shared `proofs::util` module providing bounded Kani generators for tribles,
  PATCH entries, and small commit DAGs, and updated the query harness to reuse
  them.
- `metadata::value_formatter` and `blobschemas::WasmCode` for attaching
  schema-level WebAssembly value formatters, plus an optional `wasm` feature
  (enabled by default in the `triblespace` facade crate) that runs them in a
  sandboxed `wasmi` interpreter with strict limits.
- `BlobCache`, a generic handle-keyed cache for blob conversions.
- `#[value_formatter]` proc macro attribute (in `triblespace-macros`) that
  compiles standalone Rust formatter functions into sandboxed WebAssembly
  modules and embeds the resulting bytes in the caller crate.
- Repository ancestor harness exercising `CommitSelector::ancestors` against
  nondeterministic DAGs generated by the shared bounded helpers.
- PATCH harness verifying entry insertion and replacement using the shared
  bounded generators.
- Documented the deterministic JSON importer flow and added regression tests
  covering integration with the fixed primitive mappings.
- Added a book chapter on importing external data formats that covers the
  JSON importers, attribute derivation, encoder callbacks, and strategies for
  extending the namespace to new formats.
- 0.6.0 release preparation checklist in `INVENTORY.md` detailing actionable
  subtasks for the remaining blockers and polish items.
- Formal verification roadmap outlining Kani, Miri, fuzzing, and simulation
  testing plans in the book.
- Expanded the roadmap with an explicit invariant catalogue, spelling out the
  PATCH/ByteTable invariants exercised by `Branch::modify_child`, clarifying the
  value-schema guarantees around `TryFromValue`, and synchronised follow-up
  tasks in `INVENTORY.md`.
  PATCH/ByteTable invariants exercised by `Branch::modify_child`, and synchronised
  follow-up tasks in `INVENTORY.md`.
- Documented the set-combinator commit selectors (`union`, `intersect`,
  `difference`) in the book with usage examples.
- `_?ident` scoped variables for `pattern!` and `pattern_changes!`, enabling
  fresh bindings without declaring them in `find!` heads, along with
  documentation and tests.
- `temp!` macro for allocating hidden query variables across constraints, plus
  documentation and regression tests covering cross-pattern joins.
- Clarified the `and!` example in the Query Language chapter to show how
  membership helpers can pair with pattern constraints drawn from a different
  collection.
- Documented repository storage backends and added a book page tracking future
  documentation improvements.
- Clarified the `Trible` layout, indexing strategy, and edge semantics in the
  deep dive chapter of the book.
- Expanded the documentation backlog with notes on remote object-store conflict
  handling, succinct archive indexes, and extending regular path engines.
### Changed
- `Attribute` now retains its declared name, uses the field name for dynamic
  attributes, and relies on the `Metadata` trait to emit attribute metadata in
  both code-generated and runtime scenarios.
- Simplified the attribute constructors to `from_id`, `from_id_with_usage`, and
  `from_name`, removing `from_id_with_name`/`from_handle` in favor of explicit
  usage metadata and internal handle derivation.
- Simplified attribute naming by replacing the internal `AttributeName` enum
  with an optional `Cow<'static, str>`, keeping const-friendly static ids while
  storing dynamic field names directly.
- Replaced the `ValueSchema::VALUE_SCHEMA_ID` and `BlobSchema::BLOB_SCHEMA_ID`
  associated constants with `ConstMetadata::id()` across value and blob schemas,
  preserving existing identifiers and deriving composite `Handle` schema IDs
  deterministically from their hash protocol and blob schema components.
- Made `HashProtocol` extend `ConstMetadata` so protocol identifiers come from
  the unified metadata API alongside value and blob schemas.
- Documented why schema identifiers remain regular functions until `blake3`
  exposes a const-friendly hashing API for composite handle schemas.
- Removed the `ValueSchema::BLOB_SCHEMA_ID` associated constant and stopped
  emitting attribute metadata that relied on blob schema coupling.
- Glossary chapter in the book for quick reference to core terminology.
- Expanded the Identifiers chapter with a `local_ids` + `IdOwner` workflow
  example showing how to borrow freshly minted IDs in queries.
- `nth_ancestor` commit selector corresponding to Git's `A~N` syntax and
  documentation updates.
- `parents` commit selector corresponding to Git's `A^@` syntax.
- `INVENTORY.md` file and instructions for recording future work.
- README now links to the corresponding chapters on https://triblespace.github.io/triblespace-rs.
- `Constraint::influence` method for identifying dependent variables.
- Documentation and examples for the repository API.
- Book section showing how to stage and fetch workspace blobs with `Workspace::put`
  and `Workspace::get`.
- Guidance on integrating custom constraints with external data sources in the book.
- Garbage-collection chapter now shows how `BranchStore::branches`, `reachable`,
  and `transfer` work together to enumerate branch roots and traverse blobs in
  practice.
- Clarified the garbage-collection root description to highlight that the
  traversal retains everything reachable from enumerated branch metadata.
- Remote store workflow example in the book showing how to open
  `ObjectStoreRemote` repositories and clarifying that no explicit close is
  required for remote backends.
- `union`, `intersect`, and `difference` commit selectors that expose PATCH set
  operations through the `CommitSelector` interface.
- Documented `TribleSet` set operations and monotonic semantics in the Trible
  Structure chapter.
- Test coverage for `branch_from` and `pull_with_key`.
- Migrated `SuccinctArchive` to new `jerky`/`anybytes` APIs and added
  serializable metadata.
- `_?name` scoped variables for `pattern!`/`pattern_changes!` along with
  documentation and tests demonstrating their use.
- Implemented `ToBlob`/`TryFromBlob` for `SuccinctArchive`, enabling archive
  serialization as a blob.
- `Pile::restore` method to repair piles with trailing corruption.
- Documented zero-length blob support and added tests for empty blob insertion and retrieval.
- `with_sorted_dedup` constructor for universes to build from already sorted,
  deduplicated value sequences.
- Troubleshooting table in the repository workflows chapter covering common
  push, branch, and pull failure modes.
- Book section documenting how to manage multiple signing identities with
  `Repository::set_signing_key`, `Repository::create_branch_with_key`, and
  `Repository::pull_with_key`.
- Reworked Chapter 1 introduction to clarify Trible Space's goals, distinguish
  fixed-width tribles from blob payloads, and guide readers through the rest of
  the book.
- Dedicated "Portability & Common Formats" chapter in the book capturing value
  schemas, identifiers, and conversion guidance, referenced from the `Value`
  crate docs, and closing out the documentation backlog request to move this
  material out of the API reference.
- Chapter exploring the TribleSpace type algebra linking `attributes!`,
  `entity!`, and query semantics.

### Changed
- Expanded the Pile Blob Metadata chapter with an accurate header field
  breakdown, timestamp conversion example, and details on lazy validation.
- Corrected the BlobMetadata description to state it surfaces the timestamp and
  length fields from the header.
- Expanded the Pile Format chapter with a restore-first operational workflow,
  corrected usage example showing a restore-after-open startup without an
  explicit refresh, and detailed record field breakdowns.
- Clarified that `Pile::restore` already applies intact records before
  truncating and that reader/branch helpers refresh automatically, so manual
  refreshes are only needed when scanning between operations.
- Corrected the PATCH deep-dive chapter so its descriptions of persistence,
  node layout, resizing, and hash maintenance match the implementation.
- Clarified the PATCH deep-dive resizing description to avoid implying growth
  beyond the 256-entry table.
- Clarified the PATCH hash-maintenance discussion so hash comparisons
  short-circuit on matches and walk on mismatches.
- Expanded the deep-dive blobs chapter with guidance on when to use blob
  storage, how handles relate to schemas, and annotated examples.
- Corrected the Trible Structure deep dive to describe how `TribleSet::union`
  mutates its receiver while the other set operations return new views.
- Expanded the deep-dive philosophy chapter with explicit guiding principles,
  clarified how asynchronous backends surface through blocking entry points, and
  grounded the practical implications in the currently available tooling.
- Expanded the Formal Verification roadmap with a stack overview, contributor
  workflow guidance, and milestone tracking suggestions.
- Expanded the garbage-collection chapter with guidance on choosing root sets,
  operational tips, clearer explanations of the conservative traversal, and a
  scan description that matches the implementation (including the fact that the
  walker scans every blob in 32-byte chunks because the store is type-agnostic).
- Reworded the garbage-collection safety-margin tip to emphasize that
  near-impossible hash collisions make extra roots a conservative way to protect
  reachable data.
- Refined the garbage-collection example loop to iterate branch IDs directly
  when collecting roots from `BranchStore::branches`.
- Reworked the documentation improvement chapter with prioritised sections and
  contribution guidance for future book updates.
- Standardised citation formatting in the documentation backlog to match the
  book's reference style.
- Refined the Descriptive Typing chapter with accurate workspace lifecycle
  guidance, corrected `find!` pattern syntax, clearer advice on structuring
  ad-hoc projections and strongly discouraging long-lived typed wrappers,
  updated the
  manager-owned repository DI section to emphasize using short-lived
  `&mut Repository<_>` borrows, handing out task-scoped `&mut Workspace<_>`
  handles, highlighting how multiple mutable workspaces coexist over a
  single repository, clarified that cloning an already-fetched blob is cheap
  compared to the cost of retrieving it from storage, and corrected the
  description of composable clauses so it no longer suggests optional
  attribute matching.
- `json_import` benchmark now publishes separate element- and byte-throughput
  groups, precomputing importer-specific trible counts so Criterion reports
  both perspectives.
- Inlined the JSON importer's trible insertion helper to avoid an extra
  function hop when staging statements.
- Updated both JSON importers to stage objects as `TribleSet`s and have the
  entrypoints union the staged results after validation, removing the
  `PendingJsonObject` scaffolding.
- Inlined the deterministic JSON importer's raw trible helper so hashed
  statements insert without bouncing through an additional function.
- Constructed deterministic JSON importer statements with `Trible::new`
  instead of hand-assembling raw buffers each time a pair flushes.
- Restored the JSON importer's `PrimitiveRoot` error for non-object roots,
  dropped the deterministic importer nil-id guard, and added regression tests
  covering both cases.
- Allowed both JSON importers to accept top-level arrays by returning multiple
  root entities, keeping primitive roots rejected while permitting batches of
  objects.
- Simplified the JSON importer API to return root ids while exposing data and
  metadata via accessors on `JsonObjectImporter`, avoiding an extra wrapper type.
- Simplified JSON importer error diagnostics to avoid tracking JSON paths in
  the hot import loop.
- JSON importers now emit `metadata::name` and `metadata::attr_value_schema`
  tribles when minting attributes so imported datasets carry their own schema
  descriptions.
- Attribute metadata emission now uses the public `entity!` macro so schema
  descriptions are assembled with the same ergonomic syntax exposed to
  consumers.
- Both JSON importers now merge their cached attribute metadata into the
  result set after converting documents instead of inserting metadata entries
  mid-import, keeping the hot path lean while still returning the schema
  descriptors for every derived attribute.
- JSON importers now compute metadata tribles directly from their attribute
  caches at the end of each import, avoiding duplicate metadata storage while
  preserving the descriptors for all derived fields.
- Clarified the importing guide to explain that `metadata()` returns attribute
  descriptors generated from the cached ids after an import completes.
- Documented how deterministic JSON imports collapse repeated subdocuments,
  explaining why they can outperform nondeterministic runs even with cheap ID
  generators.
- JSON importers retain the accumulated tribles inside the importer, exposing
  `data()`/`metadata()` accessors along with `clear_data()` and `clear()` helpers
  so multiple JSON documents can be staged—or reset entirely—before reading the
  results instead of returning a fresh `TribleSet` from each `import_value`
  call.
- Replaced the JSON importer's `anyhow` dependency with a lightweight encoder
  error wrapper so callbacks stay flexible without pulling in extra baggage.
- Dropped the JSON importer's `JsonValueKind` helper and folded the top-level
  type detection directly into the error path to trim unused indirection.
- Parameterized the JSON importer's ID generation so callers can provide
  deterministic sequences via `with_id_generator`, and added a regression test
  covering custom generators.
- Cached JSON importer attributes per field name so repeated values reuse the
  same hashed identifiers without recomputing them.
- Expanded the Schemas chapter with validation examples, clarified how schema
  identifiers power cross-language tooling and deterministic attribute imports,
  outlined schema evolution best practices, and corrected the built-in blob
  schema references for succinct archives.
- Expanded the Incremental Queries chapter with practical guidance on
  preparing delta sets, reusing `TribleSet` set operations, and tying the
  workspace and local-buffer stories together through the shared set
  algebra that powers both workflows.
- Expanded and corrected the Atreides Join chapter with a structured
  walkthrough: it now explains the constraint interface, details the
  Jessica/Paul/Ghanima/Leto heuristic ladder, clarifies what quantity each
  variant estimates, describes the ordering heuristics used by the guided
  search, motivates the worst-case optimal guarantee, and clarifies how
  per-variable estimates are derived in the worked example while tying the
  introduction back to the broader worst-case optimal join literature.
- Macro instrumentation now records the entire span of each invocation in a
  single `source_range` attribute instead of separate line and column values.
- Implemented `ToValue<LineLocation>` for `proc_macro::Span` so metadata
  wrappers can hand spans directly to `entity!` without manual tuple
  construction.
- Attribute metadata emission no longer attempts to resolve value/blob schema
  identifiers, sticking to the information reliably available at macro
  expansion time.
- Metadata emission callbacks now receive a mutable context exposing the
  workspace, invocation ID, and tokens so wrapper macros can commit additional
  metadata directly without reopening the repository.
- Metadata emission now commits records to the configured repository branch
  instead of appending raw archives to a standalone pile, aligning the
  instrumentation with the standard storage workflow and renaming the
  environment variable knobs accordingly.
- Regenerated the macro instrumentation attribute identifiers from
  command-line randomness to document their provenance and avoid
  hand-crafted values.
- Metadata instrumentation now reuses the shared hex parsing helpers when
  decoding signing keys and branch identifiers from the environment and
  requires exact hexadecimal strings without a prefix, eliminating bespoke
  sanitization logic in the wrapper crate.
- Reworked the Query Engine chapter to describe the in-search Atreides
  cardinality estimates, clarify how constraints cooperate at runtime, and remove
  references to a nonexistent planner.
- Clarified how the Query Engine search loop derives join variants from
  cardinality heuristics, documented the role of `confirm` inside `and!`, and
  replaced the chapter's query example with a runnable snippet that mixes
  `pattern!` constraints with a `HashSet` filter.
- Updated the architecture overview and trible structure deep dive so they
  describe join ordering as a search-loop choice driven by constraint
  heuristics instead of a separate planner.
- Clarified in the Architecture chapter that blob stores, not repositories, perform
  deduplication of uploaded content.
- Corrected the push/pull arrows in the Architecture diagram to match the actual
  workspace and repository data flow.
- Refined the Architecture diagram and explanation to match
  `Repository::pull`, `Workspace::commit`, and `Repository::try_push`
  responsibilities.
- Reworked the Architecture diagram again to restore the approachable
  workspace overview, clarify the `commit`/`add_blob` interactions, and ensure
  the push arrow flows from the workspace into the repository box.
- Tightened the Architecture diagram so `push/try_push` rises from the
  workspace, `pull` flows back from the repository, and the workspace box now
  highlights concise `commit`/`add_blob` annotations plus a `checkout` link to
  the application layer, then nudged the arrow spacing and arrowheads for
  clearer alignment.
- Re-reviewed the book and codebase to tighten the Glossary definitions:
  clarified how attributes carry their schemas via `attributes!`, explained
  that schemas stay language agnostic instead of binding to Rust types, noted
  that blobs hold archived `TribleSet`s and commit metadata, documented commits
  as `SimpleArchive` blobs with signed metadata, and highlighted identifier
  ownership in the entity entry alongside the existing PATCH description.
- Reorganized the workspace so the new `triblespace` crate exposes the public
  prelude, examples, and documentation while the implementation lives in
  `triblespace-core` with procedural macros in `triblespace-core-macros`,
  enabling future proc-macro crates to depend on the core without cyclic
  dependencies.
- Moved the README regression test and Kani proof harnesses into the
  `triblespace` facade crate so `triblespace-core` stays lean for proc-macro
  consumers while the public API remains thoroughly exercised.
- Expanded Chapter 1 of the book with clearer motivation, reader guidance, and
  an outline of the subsequent chapters. Streamlined the "Why Trible Space
  exists" section so it stays focused on the data-management pains Trible Space
  solves and how pairing blobs with fine-grained facts addresses them, and
  reworded the flexible querying description to show how a single query blends
  trible sets, succinct indexes, and Rust collections such as hash maps.
- Aligned the README regression test with the expanded library conflict resolution walkthrough so documentation stays exercised.
- Regenerated the quick-start alias attribute ID with a CLI-generated value so the README, book, and regression test stay in sync.
- Unified the getting started walkthrough around the library example, showing `push` for automatic retries, `try_push` for manual conflict handling, and updating the README snippet to match.
- Expanded the book's getting started chapter with clearer step-by-step setup,
  execution instructions, and explanations of the repository workflow pieces.
- Restored the README's quick-start example while keeping the expanded
  walkthrough in the getting started chapter so newcomers can skim or dive
  deeper as they prefer.
- Contributor guidelines now require reading the entire `./book` before starting each new task to stay aligned with project concepts.
- `proofs::util::bounded_id` now rejects the nil sentinel with `kani::assume`
  to keep identifier generation unbiased while ensuring exclusivity checks stay
  sound in verification harnesses.
- Expanded the Developing Locally chapter with setup steps, workflow scripts,
  and book rebuild instructions.
- Reconciled the duplicated Query Language edits by combining the reorganised
  introduction, conversion guidance, simplified `ignore!` syntax that always
  captures the surrounding query context while still minting distinct
  temporary variables, richer `or!` and `pattern!` examples, and updated
  regular path query coverage.
- Clarified the regular path example to use `temp!` when hiding an endpoint so
  the traversal still participates in follow-up constraints without projecting
  the hidden binding.
- `ignore!` now always infers its context from `find!`/`matches!`. Use
  [`IgnoreConstraint::new`](https://docs.rs/tribles/latest/tribles/query/ignore/struct.IgnoreConstraint.html)
  directly when building bespoke constraints outside those macros.
- `temp!` now mirrors `ignore!` by taking both the tuple-style binding list and
  the scoped expression, so helper variables introduce their own temporary
  block without wrapping the surrounding query body manually.
- `temp!` no longer accepts explicit type annotations. Hidden bindings never
  project into the result tuple, so their value schemas are inferred entirely
  from how they are used inside the scoped expression.
- Documented `temp!` alongside the other built-in macros in the Query Language
  chapter's constraint table so readers can spot it at a glance.
- Clarified the `ignore!` documentation to highlight that ignored bindings are
  never solved or unified, showing how triple-style constraints can drop unused
  positions while branches that reference only ignored variables never even get
  scheduled.
- Streamlined the `ignore!` partial-projection example by trimming unrelated
  namespace discussion and added an introduction note that highlights how the
  macros wrap the underlying constraint builders for manual use.
- Query Language chapter now gives `or!` its own subsection, calls out
  `_?name` placeholders in `pattern!`/`pattern_changes!` as an alternative to
  `temp!` when hidden helpers stay within a single pattern, clarifies that each
  branch behaves as an independent constraint whose matches are all retained so
  the overall query stays monotonic, documents that all branches must reference
  the same variable set, and notes that mismatches panic at runtime.
- Documented the `.is(...)` constant constraint alongside the other built-in
  operators, added a dedicated subsection showing how to pin bindings,
  highlighted that `pattern!`/`pattern_changes!` already materialise constant
  constraints for literal values, and pointed readers to membership helpers
  such as `.has(...)` when accepting several literals.
- Added `pattern!` and `pattern_changes!` to the built-in constraints table,
  noting that incremental patterns emit only additions and pointing readers to
  the Incremental Queries chapter for the full evaluation workflow.
- Clarified the `has` membership entry so it points to `ContainsConstraint`
  implementors like set-style collections while steering triple sources toward
  `pattern!`.
- Added an "Intersections (`and!`)" subsection to the Query Language chapter
  covering how conjunctions combine clauses, share bindings, and nest within
  other combinators.
- Normalized the Descriptive Typing chapter to use consistent Markdown headings
  and remove unused front matter.
- Re-reviewed the type algebra chapter, linking its claims directly to the
  `Attribute`, `TribleSet`, and query constraint implementations for accuracy.
- Softened the Type Algebra chapter summary to describe the design without
  value-laden language.
- Rephrased the Type Algebra chapter's closing sentence to highlight surface
  simplicity backed by rich type theory.
- Clarified `PATCH::iter_ordered` and `PATCHOrderedIterator` documentation to
  describe the full tree-order traversal without a prefix filter and point to
  the prefix iterator for filtered traversal.
- Reframed the identifiers deep-dive chapter to highlight the abstract/semantic
  and intrinsic/extrinsic axes, expand the embeddings discussion, and provide
  clearer guidance on choosing identifier families.
- Audited the identifier taxonomy guidance to align the RNGID/UFOID/FUCID
  comparison with their implementations and fix crate-qualified links in the
  table.
- Trimmed the Portability & Common Formats chapter by removing the "Why this
  chapter lives in the book" subsection after documenting the move from the
  `Value` module docs.
- Documented the `path!`, `attributes!`, and `pattern_changes!` procedural
  macros in the `tribles-macros` crate overview.
- `attributes!` procedural macro now resolves the caller's crate path so
  downstream users can depend solely on the `triblespace` facade when
  generating attribute constants.
- Reframed commit range selectors so `start..end` walks from the end selector
  until encountering a commit yielded by the start selector, reducing
  redundant ancestor exploration and making the traversal cost explicit.
- Query Engine chapter now directs readers to the crate-level `pattern!` and
  `entity!` macros and shows how to import them via the prelude.
- Removed the outdated note that parentheses "force" literals in the getting
  started guide now that the macros rely on regular Rust expression syntax for
  literal detection.
- Commit selectors chapter now highlights range semantics, composability, and
  Git parity to help readers choose the right selector for their workflow,
  clarifies that selectors only pick commits while `Workspace::checkout`
  materializes the `TribleSet`, refreshes the composition example to layer
  entity filters over a time range, and shows how to combine selectors with the
  built-in set-operation helpers.
- Pinned `anybytes` and `jerky` to specific git revisions via a crates.io patch
  so all dependents use a single source and API surface.
- Refined the selector debugging guidance to encourage validating each layer
  independently before composing them with the built-in set-operation helpers.
- Documented the trade-off that empty start selectors rewalk the full history,
  and showed how incremental queries can reuse the previous head commit as the
  next range boundary to avoid repeating the walk.
- Corrected the commit selector range description to note inclusive end
  boundaries and clarified that selectors compose via the `CommitSelector`
  trait instead of `IntoIterator`.
- Clarified the commit selector traversal description to avoid implying a
  specific order, fixed the `ancestors(A)..B` exclusion example, and tightened
  the debugging guidance wording.
- Clarified that `find!` retrieves `ExclusiveId` bindings via `FromValue` and
  that restricting queries with `local_ids` keeps the conversion safe.
- Getting started guide now demonstrates defining custom attributes alongside
  the quick-start example, hides doc-test-only cleanup, and exercises the
  quick-start snippet as a runnable doc test.
- Updated README and book code samples to use the public `entity!`/`pattern!`
  macros so snippets copy-and-paste outside the crate.
- Updated the README and book examples to use `Repository::create_branch` plus
  `pull` instead of the removed `branch` helper when initializing workspaces.
- Combined the README quick-start and standalone example into one repository
  workflow that stages, queries, and pushes a dataset backed by freshly minted
  `attributes!` definitions instead of the shared literature namespace.
- Updated the release preparation inventory to call out multi-`attributes!`
  module examples instead of cross-namespace guidance.
- Pruned completed 0.6.0 release checklist items (prefix guards, succinct archive parity,
  pile property tests) from the inventory after auditing the codebase.
  - README walkthrough and regression test now commit the staged dataset by value
    instead of cloning it before submission.
  - Updated `SuccinctArchive` to use `BitVectorDataMeta` for prefix bit vectors.

### Fixed
- Reinstated the `ValueSchema` documentation that notes hash handles still carry
  their referenced blob schema type parameter.
- Updated deterministic JSON importer metadata tests to align with attribute
  metadata now emitting only value schema descriptors.
- Added the missing `blake3` dev-dependency and adjusted the JSON importer
  benchmark to allocate owned strings and convert JSON numbers via
  `f256::from`, restoring the json benchmarks after recent refactors.
- Updated JSON importer benchmarks, core tests, and book snippets to ensure the
  `LongString` generic parameter stays attached to the trait, fixing
  compilation failures introduced by the new benchmark and documentation
  examples. Book snippets now rely on type inference for `to_blob()` to match
  idiomatic usage.
- Corrected the JSON import benchmark to use the re-exported
  `valueschemas::Blake3` handle schema so it compiles again.
- Added the missing `serde_json` and `f256` dev-dependencies so the JSON import
  benchmark builds successfully.
- Buffered the JSON importers so encoding errors roll back an entire import
  instead of leaving partially imported tribles in the accumulated set.
- Routed the JSON importer staging helpers through a shared temporary
  `TribleSet` so field emitters avoid building intermediate sets before the
  batch commits.
- Updated the procedural macros to resolve either the `triblespace-core` or
  `triblespace` crate path automatically so downstream users can rely on the
  facade crate without declaring extra dependencies.
- `SuccinctArchive` now derives domain metadata via `Serializable` instead of storing raw handles.
- `SuccinctArchive` now retains a handle to a contiguous byte area so blob serialization clones the underlying bytes without rebuilding.
- Simplified blob deserialization by reading archive metadata via `Bytes::view_suffix`.
- `SuccinctArchive`'s `Serializable` implementation now reports concrete
  `jerky::error::Error` values instead of relying on `anyhow`.
- Removed the custom empty `WaveletMatrix` metadata workaround now that the
  builder accepts zero-length sequences.
- `SuccinctArchive::from` now seeds wavelet matrices without guarding against
  empty archives because the builder handles zero-length iterators.
- Verified the wavelet-matrix builder path against empty archives via
  `./scripts/preflight.sh` after the jerky upgrade.
- `OrderedUniverse` now stores values as `View<[RawValue]>` for zero-copy access.
- Simplified `OrderedUniverse::with_sorted_dedup` to always collect incoming
  values before writing them into the reserved section, avoiding reliance on
  unstable iterator detection.
- Universes now allocate their own byte sections via a `SectionWriter`, so callers only pass an iterator. `CompressedUniverse::with` no longer clones its values.
- `SuccinctArchive` constructs universes with `with_sorted_dedup`, avoiding an extra sort/dedup pass when the caller already guarantees ordering.
- Updated the repository workflow documentation to use `Repository::create_branch`
  and provide a runnable blob staging example.
- Expanded the repository workflows chapter with an overview of repository
  initialization, branching conventions, and guidance on choosing between
  `push` and `try_push`.
- Getting started guide now highlights the need to close pile-backed repositories so callers can handle flush errors explicitly.
- README example now inlines the shared `tribles::examples::literature` namespace so the getting started walkthrough and crate examples stay aligned without depending on internal modules.
- README walkthrough and regression test keep the namespace name `literature` to match the shared example module.
- `with_sorted_dedup` now accepts iterators so compressed universes can build domains without materializing values.
- `SuccinctArchiveMeta` now accepts the domain's serialized metadata type,
  removing its hardcoded `SectionHandle<RawValue>` dependency.
- Architecture chapter now explains the system layers, copy-on-write behaviour,
  and how repositories coordinate blob and branch stores.
- `SuccinctArchiveMeta` bounds metadata types with jerky's `Metadata` marker
  to guarantee zero-copy-safe layouts.
- `CompressedUniverse` now relies solely on jerky's `DacsByte` and a section-
  backed fragment table, enabling fully zero-copy serialization via
  `Serializable`.
- Documented that branch updates do not ensure referenced blobs exist, enabling
  piles to serve as head-only stores.
- Clarified repository workflow docs with a sidebar highlighting
  `repo::transfer` alongside `BlobStoreKeep::keep`, including
  garbage-collection scenarios that only copy live blobs.
- Removed the suggested branch conventions subsection from the repository
  workflows chapter so the page concentrates on API behavior and storage
  guidance.
- Clarified that multiple pile writers require filesystems with atomic append
  semantics; noted unsupported filesystems in documentation.
- Streamlined the merge troubleshooting note to highlight
  `MergeError::DifferentRepos` and the `reachable` + `repo::transfer` steps for
  cross-repository merges.
- Documented the pile as a write-ahead log database ("WAL-as-a-DB").
- Rewrote the pile blob metadata chapter to describe the `BlobMetadata`
  API and linked it from the pile format documentation.
- Documented that the pile is an immutable append-only log: only the un-applied tail is validated and mutating existing data is undefined behavior.
- Removed in-flight blob tracking. `Pile::put` now holds a shared lock,
  refreshes before writing, then reads back its blob with `apply_next` to ensure
  it was indexed. `Pile::update` similarly verifies the written branch record
  using `apply_next` under its exclusive lock.
- `Pile::close` now consumes the pile and manually drops its fields to bypass
    `Drop`, which always warns when a pile is not explicitly closed.
- `Pile::close` now drops all fields before returning the result of `flush`,
  ensuring resources are cleaned up even if flushing fails.
- `Pile::refresh` now aborts if the pile file shrinks below data already
  applied, guarding against truncated data.
- Documented that truncation below `applied_length` invalidates previously
  issued `Bytes`, so only the un-applied tail is checked for corruption and
  shrinkage into validated data requires aborting.
- Clarified that shrinkage into already applied data triggers an immediate
  process abort to avoid undefined behavior from dangling `Bytes` handles.
- `Pile::refresh` acquires a shared file lock while scanning to avoid races with
  `restore` truncating the file.
- `Pile::restore` truncates the pile without rescanning after truncation,
  removing a redundant refresh pass.
- `Pile::refresh` uses a simple `insert` for new blob index entries.
- `Pile::update` no longer flushes or `sync_all`s automatically; callers must
    invoke `flush()` for durability.
- `Pile::open` now returns an empty handle without scanning the file. Call
  `refresh` to load existing data or `restore` to repair corruption. The
  `try_open` helper was removed.
- Additional unit tests for `Pile` blob iteration, metadata, and conflict handling.
- `Workspace::checkout` helper to load commit contents.
- Documentation and example for incremental queries using `pattern_changes!`
  plus additional tests.
- `pattern!` now implemented as a procedural macro in the new `tribles-macros` crate.
- Regression test ensuring `PATCHOrderedIterator` returns keys in sorted order.
- `entity!` now implemented as a procedural macro alongside `pattern!`.
- `ThompsonEngine` implementing a new `PathEngine` trait for regular path queries,
  and `RegularPathConstraint` is now generic over `PathEngine`.
- `reachable` iterator, `transfer` helper, and `potential_handles` expose the
  conservative blob traversal for composition. `BlobStoreKeep::keep` and
  `MemoryBlobStore::keep` now retain blobs by handle iterators.
- Implemented `size_hint`, `ExactSizeIterator`, and `FusedIterator` for `PATCHIterator` and `PATCHOrderedIterator`.
- Compile-time check restricting builds to 64-bit little-endian targets.
- `PileReader` now reconstructs blob data from the underlying memory map,
  and `IndexEntry::Stored` tracks offsets and lengths instead of holding `Bytes` directly.
- Regression test ensures `PATCH::iter_ordered` yields canonically ordered keys.
- `PATCH::replace` method replaces existing keys without removing/ reinserting.

### Fixed
- Corrected the repository workflow documentation to describe the actual
  `Repository::push` and `Repository::try_push` return values and clarify that
  remote backends expose a no-op `repo.close()`.
- Corrected the `PATCHOrderedIterator` documentation to describe its
  lexicographic key-order traversal instead of prefix iteration.
- Restored `_?ident` locals in `pattern!`/`pattern_changes!` to infer their
  value schema from usage instead of forcing `GenId`, so scoped bindings work on
  non-`GenId` attributes again.
- Resolved hygiene issues in `pattern!`/`pattern_changes!` so user bindings like
  `__ctx` no longer collide with generated identifiers, and added trybuild
  coverage to prevent regressions.
- Corrected the blob book example to import the repository module via `tribles::repo`.
- Removed an unused `anyhow` import from the succinct archive schema.
- `SuccinctArchive::from` now handles empty `TribleSet`s and returns an
  empty archive instead of panicking.
- `CachedUniverse::search` avoids underflow when querying an empty universe.
- Opening excessively large piles now returns an error instead of panicking when calculating the mapped size.
- Regression tests verify blob bytes remain intact after branch updates and across flushes.
- `PileReader::metadata` now validates blob contents and returns `None` for corrupted blobs.
- `PileBlobStoreIter` now lazily verifies blob hashes and reports errors for invalid blobs.
- `PileBlobStoreIter` now skips missing index entries instead of ending iteration silently.
- `Pile::flush` now calls `sync_all` to persist file metadata and prevent
  potential data loss after crashes.
- `Pile::restore` now syncs the file after truncation to ensure durability.
- `Pile` requires explicit closure via `close()`; dropping without closing emits a warning.
- Debug helpers `EstimateOverrideConstraint` and `DebugConstraint` moved to a new
  `debug` module.
- Debug-only `debug_branch_fill` method computes average PATCH branch fill
  percentages by node size.
- Added a simple `patch` benchmark filling the tree with fake data and printing
  branch occupancy averages.
- Trible key segmentation and ordering tables are now generated from a
  declarative segment layout, simplifying maintenance.
- Deterministic proptest simulation tests cover multi-reader and writer pile
  operation sequences via actor-scheduled operations.
- Simulation now exercises branch updates, branch listing, and fetching
  previously stored blobs and branch heads for comprehensive pile coverage.
- Additional pile unit tests exercising branch conflicts and size limits.
- Additional unit tests cover pile blob metadata, iteration, and branch update
  conflicts.
- Additional unit tests covering pile deduplication, metadata, and branch
  update conflicts.

- `Pile` no longer requires a compile-time size limit, grows its mmap on demand,
  and `ReadError::PileTooLarge` was removed.
- Initial pile mapping now uses a page-sized (×1024) base to avoid frequent remaps.
- Mapping size now derives from the mmap length instead of an internal counter.
- Replaced fs4 with Rust std file-locking APIs.
- Declared Rust 1.89 as the minimum supported toolchain.
- Dropped the inventory item about validating externally appended blobs during
  `refresh`; blob data is verified lazily on read.
- `refresh` replaces invalid blob entries with newer candidates and verifies
  unknown duplicates before deciding whether to keep or replace them.
- `refresh` now uses `get_or_init` to compute blob validation state and
  replace invalid duplicates.
- Simplified `refresh` padding logic by using `padding_for_blob` to compute blob alignment.
- `BlobStore::reader` now returns a `Result` so implementations can signal errors during reader creation.
- Renamed pile read errors from `OpenError` to `ReadError` since they can surface during refresh.
- PATCH exposes const helpers to derive segment maps and ordering
  permutations from a declarative key layout.
- `Entry` now supports an optional value via `with_value`, preparing `PATCH`
  for key-value mappings.
- Set semantics now use the zero-sized unit `()` value instead of a dummy
  byte to avoid extra storage.
- `PATCH::get` retrieves the value associated with a key, if present.
- `Leaf` stores the associated value and `PATCH`/`Head`/`Branch` now carry a
  value type parameter so keys can map to arbitrary payloads.
- Moved the value type parameter to the end of generic parameter lists for a
  more ergonomic `PATCH<KEY_LEN, Order, Value>` API.
- Documented that hashing and equality ignore leaf values and added a
  regression test verifying patches with identical keys but different values
  compare equal.
- Introduced `key_segmentation!` and `key_schema!` macros to emit
  `KeySegmentation` and `KeySchema` implementations from those declarative
  layouts.
- Added `byte_table_resize_benchmark` measuring average fill ratios that cause
  growth for random vs sequential inserts. It now tracks the number of elements
  inserted at each power-of-two table size to compute per-size and overall
  averages over many random runs.
- Preallocated the resize counts vector to avoid repeated allocations during
  the benchmark.
- Per-size results now include sizes that never triggered growth so the output
  has no gaps.
- Documented PATCH's cuckoo-hashing compression as an alternative to ART-style
  node compression, explained its compressed-permutation hash with an identity
  first permutation and a random second permutation and why the smallest and
  largest nodes are always fully occupied, and included benchmark fill ratios in
  the book.
- Annotated the benchmark output to highlight path compression in the size-two
  case and that the identity hash lets 256-ary nodes store all 256 children.
- `entity!` subsumes the old `entity_inner!` helper; macro invocations can
  optionally provide an existing `TribleSet`.
- Procedural `namespace!` macro replaces the declarative `NS!` implementation.
- Implemented a procedural `delta!` macro for incremental query support.
- Expanded documentation for the `pattern` procedural macro to ease maintenance, including detailed comments inside the implementation.
- Expanded Query Language chapter with iterator examples and clarified that
  `ignore!` removes the named variables from planning while the scoped bindings
  still unify inside the ignored expression, making it easy to drop value
  columns from multi-position constraints without losing the join on the
  remaining variables.
- `EntityId` variants renamed to `Var` and `Lit` for consistency with field patterns.
- `Workspace::checkout` now accepts commit ranges for convenient history queries.
- Git-based terminology notes in the repository guide and a clearer workspace example.
- Expanded the repository example to store actual data and simplified the conflict loop.
- Failing test `ns_local_ids_bad_estimates_panics` shows mis-ordered variables return no results when a panic is expected.
- Diagram and explanation of six trible permutations and shared leaves for skew‑resistant joins.
- Additional example in the Commit Selectors chapter demonstrating how to
  compose `filter` with `time_range`.
### Changed
- `Branch::upsert_child` now always refreshes `childleaf`, removing the `replaced_leafchild` check.
- Blob index now uses value-aware `PATCH` for cheap reader clones.
- Inlined `refresh_range` logic into `refresh`, removing the partial-range helper.
- Blob appends now issue a single `write_vectored` `O_APPEND` call to stream header, data and padding without extra copies or retries.
- Simplified vectored blob appends by always including a padding slice.
- Branch updates now perform `flush → refresh → lock → refresh → append → unlock` directly instead of queuing.
- Branch headers are written with a single `write` call to avoid partial updates.
- Max-size checks and mmap offsets now derive from the file's actual length instead of tracked counters.
- Restored an `applied_length` tracker to incrementally refresh new blobs and branches without rescanning the entire pile.
- Blob inserts now compare the write start with the previous `applied_length`, ingesting any intervening records before advancing.
- `refresh` now uses the same framing parser as `try_open` to detect truncated or malformed records while deferring blob hash checks to reads.
- `try_open` now reuses `refresh` for log scanning, unifying corruption checks.
- `succinctarchive` schema is now gated behind an optional `succinct-archive`
  feature until it aligns with upstream `jerky` APIs.
- `refresh` retains existing blob entries when encountering duplicates instead of
  replacing validated records.
- `refresh` now uses `PATCH::replace` to update blob entries without explicit remove/insert.
- Expanded commit selector documentation with an overview, example and clearer
  wording about loading commits from a workspace.
- Temporarily gate the `SuccinctArchive` schema behind a feature to restore
  compilation while its Jerky dependency is updated.
- Expanded repository workflows chapter with clearer branching steps and a
  dedicated history section.
- Expanded Schemas chapter with additional context on schema identifiers and runtime lookup.
- Renamed `mask!` macro to `ignore!` for clarity.
- Expanded the Atreides Join chapter with an example, clearer algorithm explanations, and a note that random access remains only for confirming candidates.
- Rephrased Atreides Join discussion of sorted indexes to highlight efficient value lookup.
- Gave each Atreides join variant a descriptive name alongside its Dune nickname.
- Clarified the query engine book chapter with improved wording and examples.
- Expanded discussion on RDF's per-value typing limitations in the query engine chapter.
- Expanded Architecture chapter's blob storage section for clearer responsibilities and examples.
- Expanded the "Developing Locally" book chapter with guidance on helper scripts and local setup.
- Expanded the "Getting Started" book section with dependency setup and run instructions.
- PATCH infix and segment-length operations now require prefixes to align with
  segment boundaries.
- `KeySchema` and `KeySegmentation` now expose translation tables as associated const arrays instead of methods.
- Removed `key_index`, `tree_index`, and `segment` helper methods in favor of direct const-table lookups and tied `KeySchema` to its `KeySegmentation` with an explicit segment permutation.
- `KeySchema` now declares its `KeySegmentation` via an associated type instead of a separate generic parameter.
- Renamed `KeyOrdering` trait and `key_ordering!` macro to `KeySchema` and `key_schema!` for clearer terminology.
- Blob writes are now synchronous; `put` records an `InFlight` entry so repeated writes of the same blob are deduplicated until a refresh.
- Pile size limits are enforced during `refresh` rather than on each write.
- `ByteTable` plans insertions by recursively seeking a free slot and shifts entries only after a path is found, returning the entry on failure so callers can grow the table.
- ByteTable's planner tracks visited keys with a stack-allocated bitset to avoid heap allocations.
- Simplified the planner and table helpers for clearer ByteTable insertion code.
- Replaced redundant option check with an `expect` when traversing full buckets in
  the ByteTable planner.
- Restored the simpler `ByteSet` and inlined bucket checks to reduce indirection in the planner.
- Removed the reified `ByteBucket` abstraction and indexed buckets directly in the byte table.
- `ByteSet` now stores raw `[u128; 2]` bitsets instead of relying on `VariableSet`.
- Detailed query engine documentation moved from the `query` module to the book, leaving a concise overview in code.
- Moved verbose inline documentation for Pile, Trible, Blob and PATCH modules
  into the book.
- Expanded Trible Structure deep-dive with design rationale and advantages
  previously kept inline.
- Added remaining rationale from the blob, patch, pile and schema docs to the
  corresponding book chapters so code comments stay concise without losing
  detail.
- Expanded the incremental queries chapter with step-by-step delta evaluation
  and clearer `pattern_changes!` guidance.
- Refined the book's introduction with a clearer overview of Trible Space and
  its flexible, lightweight query engine, plus links to later chapters.
- Simplified blob length handling in `Pile::refresh` by relying on
  `take_prefix`'s implicit bounds checking.
### Removed
- `nth_parent` commit selector and helper; parent-numbering is not planned.
- Unused `crossbeam-channel` dependency.
### Fixed
- Detect oversized blob headers whose declared length exceeds the file size.
- Restored atomic vectored blob appends and single-call branch writes; errors
  if any bytes are missing.
- Removed duplicate `succinct-archive` feature declarations that prevented
  builds.
- Corrected blob offsets in `Pile` so retrieved blobs no longer include headers or
  branch records.
- Scheduled branch writes through the pile's write handle to avoid orphaned
  branch heads when crashes occur before pending blobs flush.
- Applied branch head updates immediately and sized branch records using
  `size_of` to preserve compare-and-swap semantics without magic numbers.
- Fixed compiler warnings by clarifying lifetime elision and ignoring
  generated imports when unused.
- Removed remaining 64-byte assumptions from blob writes by computing header
  length and padding with `size_of::<BlobHeader>()`.
- `ignore!` now hides variables correctly by subtracting them from inner constraints.
- ByteTable resize benchmark now reports load factor for fully populated 256-slot tables.
- `PatchIdConstraint` incorrectly used 32-byte values when confirming IDs, causing
  `local_ids` queries to return no results with overridden estimates.
- Documentation proposal for exposing blob metadata through the `Pile` API.
- Branch updates now sync branch headers to disk to avoid losing branch pointers after crashes.
- `IndexEntry` now stores a timestamp for each blob. `PileReader::metadata`
  returns this timestamp along with the blob length.
- Design notes for a conservative garbage collection mechanism that scans
  `SimpleArchive` values in place to find reachable handles.
- Clarified that accidental collisions are practically impossible given 32-byte
  hashes, explaining why the collector can treat any matching value as a real
  reference.
- Expanded the book's garbage collection chapter with clearer reachability
  description, traversal overview and handle-based pruning.
- Repository workflows chapter covering branching, merging, CLI usage and an improved push/merge diagram.
- Separate `verify.sh` script for running Kani verification.
- Documented conflict resolution loop and clarified that returned workspaces
  contain updated metadata which must be pushed.
- Explained BranchStore's CAS-based optimistic concurrency control in the
  repository guide.
- Property tests for `ufoid` randomness and timestamp rollover.
- Further clarified `timestamp_distance` documentation that it only works with
- Documentation for built-in schemas and how to create your own.
  timestamps younger than the ~50-day rollover period.
- Added `HybridStore` to combine separate blob and branch stores.
- Added tests for the `ObjectStoreRemote` repository using the in-memory
  object store backend.
- Implemented `Debug` for `ObjectStoreRemote` and replaced `panic!` calls
  with `.expect()` in object store tests.
- Initial scaffold for a narrative "Tribles Book" documentation.
- Build script `build_book.sh` and CI workflow to publish the mdBook.
- Expanded the introduction and philosophy sections of the Tribles Book and
  documented how to install `mdbook`.
- Documented the pile file format in the book and expanded it with design rationale.
- Expanded the pile format chapter with recovery notes and a link to the `Pile` API docs.
- Added a book chapter describing the `find!` query language, listed
   built-in constraints, and included a reusable sample dataset for
   documentation examples.
- Added an architecture chapter that explains how `TribleSet` differs from the repository layer and details branch stores and commit flow. The diagram now better illustrates the commit flow.
- Added a "Developing Locally" chapter and linked it from the README and book introduction.
- Expanded the architecture chapter with design goals, semantic background and
  cross-references to other chapters.
- Clarified that the branch store's compare-and-set operation is the only
  place-oriented update, leaving the rest of the system value oriented and
  immutable.
- Documented the incremental query plan in `INVENTORY.md` and linked it
  to a new "Incremental Queries" book chapter detailing the approach.
- Noted that namespaces will expose a `delta!` operator, similar to
  `pattern!`, for expressing changes between `TribleSet`s. The macro
  computes the difference and uses `union!` internally to apply the
  delta constraint.
 - Documented potential commit selector redesign using git-style
   reachability semantics. Added a "Commit Selectors" design note with
    a table comparing Git syntax to the planned set-based API. The table
    is now exhaustive for Git's revision grammar, using only the general
    forms. Each entry links to the official documentation and marks
    selectors that are not planned for the initial implementation.
- Noted plans for a `delta!` operator to assist with incremental
  queries. Documentation describes how it will union patterns with
  each triple constrained to the dataset delta.
- Recorded a future task to generate namespaces from a TribleSet
  description and to rewrite `pattern!` as a procedural macro.
- Documented the internal `pattern_inner!` macro with expanded usage notes.
- Added inline comments for every `pattern_inner!` rule describing what it
  matches and why.
- Added a "PATCH" chapter to the book's deep dive section explaining the trie
  implementation.
- Recorded tasks to benchmark PATCH, analyze its algorithmic complexity and
  measure real-world space usage.
- Listed candidate built-in schemas with design notes in `INVENTORY.md` for
  future implementation.
- Documented commit range semantics explaining that `a..b` equals
  `ancestors(b) - ancestors(a)` with missing endpoints defaulting to an empty set
  and the current `HEAD`.
- Commits now record a `timestamp` using `NsTAIInterval` and workspaces provide a
  `TimeRange` selector to gather commits between two instants.
- Compressed zero-copy archives are now complete.
- Incremental queries use a new `pattern_changes!` macro.
- Added a `matches!` macro mirroring `find!` for boolean checks.
- Regular path queries via a new `RegularPathConstraint` and namespaced `path!` macro.
- `path!` automata now store transitions in a `PATCH` for efficient lookups and set operations.
- Added a `filter` commit selector with a `history_of` helper.

### Changed
- Switched `anybytes` to a git dependency and used its `Bytes` integration
  to avoid copying blob data when writing to object stores.
- README no longer labels compressed zero-copy archives as WIP.
- Switched from `sucds` to `jerky` for succinct data structures and reworked
  compressed archives to use it directly.
- Construct archive prefix bit vectors using `BitVectorBuilder::from_bit`.
- Removed completed tasks from `INVENTORY.md` and recorded them here.
- Removed the experimental `delta!` macro implementation; incremental
  query support will be revisited once `pattern!` becomes a procedural
  macro.
- Split branch lookup tests into independent cases for better readability.
- `Repository::checkout` was renamed to `pull` for symmetry with `push`.
- `IntoCheckoutRange` trait became `CommitSelector` and its `into_vec` method
  was renamed to `select`.
- Updated bucket handling to advance RNG state in `bucket_shove_random_slot`.
- Clarified need for duplicate `bucket_get_slot` check in `table_get_slot`.
- Replaced Elias--Fano arrays in `SuccinctArchive` with bit vectors for
  simpler builds and equivalent query performance.
- `SuccinctArchive` now counts distinct component pairs using bitsets,
  improving query estimation accuracy.
- Domain enumeration skips empty identifiers via `select0` and prefix bit
  vectors are constructed with `BitVector` for lower memory overhead.
- Improved `Debug` output for `Query` to show search state and bindings.
- Replaced branch allocation code with `Layout::from_size_align_unchecked`.
- Removed unused `FromBlob` and `TryToBlob` traits and updated documentation.
- Documented how `MemoryBlobStore::insert` deduplicates blobs by handle in the
  deep dive chapter.
- Simplified constant comparison in query tests.
- `pattern!` now reuses attribute variables for identical field names.
- Clarified that the project's developer experience goal also includes
  providing an intuitive API for library users.
- Renamed the `delta!` macro to `pattern_changes!` and changed its
  signature to `(current, changes, [pattern])` assuming the caller
  computes the delta set.
- Documented Kani proof guidelines to avoid constants and prefer
  `kani::any()` or bounded constructors for nondeterministic inputs.
- Fixed Kani playback build errors by using `dst_len` to access `child_table`
  length without implicit autorefs.
- Introduced `ValueSchema::validate` to verify raw value bit patterns.
- Query and value harnesses use this to avoid invalid `ShortString` data during playback.
- `ValueSchema::validate` now returns a `Result` and `Value::is_valid` provides
  a convenient boolean check.
- Corrected the workspace example to merge conflicts into the returned workspace
  and push that result.
- `preflight.sh` now only checks formatting and runs tests; Kani proofs run via `verify.sh`.
- Removed instruction to report unrelated Kani failures in PRs.
- Added missing documentation for several public structs and functions in
  `blob` and `repo` modules.
- Expanded the descriptions to clarify usage of public repository APIs.
- Moved repository and pile guides into module documentation and updated README links.
- Simplified toolchain setup. Scripts install `rustfmt` and `cargo-kani` via
  `cargo install` and rely on the system's default toolchain.
- Depend on the crates.io release `hifitime` 4.1.2 instead of the git repository.
- Added a README "Getting Started" section demonstrating `cargo add tribles` and
  a pile-backed repository example.
- Documented iteration order of `MemoryBlobStoreReader`, noted workspace use of
  `MemoryBlobStore::new` and improved `Pile::try_open` description.
- Restricted `PileSwap` and `PileAux` to crate visibility.
- Repository guidelines now discourage asynchronous code in favor of
  synchronous implementations that can be parallelized.
- Renamed `ObjectStoreRepo` to `ObjectStoreRemote` in the object-store backend.
- Listing iterators for the object-store backend now stream directly from the
  underlying store instead of collecting results in memory.
- `Repository::push` now returns `Option<Workspace>` instead of the custom
  `RepoPushResult` enum, simplifying conflict handling.
- Split identifier and trible structure discussions into dedicated deep-dive book chapters.
- `preflight.sh` now verifies that the mdBook documentation builds successfully.
- Fixed book `SUMMARY.md` so preflight passes without parse errors.
- `Workspace` now exposes a `put` method for adding blobs, replacing the old
  `add_blob` helper. The method returns the stored blob's handle directly since
  the underlying store cannot fail.
- `Workspace::get` method retrieves blobs from the local store and falls back to
  the base store when needed.
- `ReadError` now implements `std::error::Error` and provides clearer messages when opening piles.
- Removed the `..=` commit range selector. The `..` selector now follows Git's
  semantics and excludes the starting commit.
- Extracted `collect_range` into a standalone function for clarity.
- Moved `first_parent` into a standalone function for clarity.
- Added a `collect_reachable` helper to gather all commits reachable from a
  starting point.
- Scalar commit selectors once again return only the specified commit.
- Introduced an `ancestors` selector to retrieve a commit and its history.
- Commit selectors now return a `CommitSet` patch of commit handles instead of a `Vec`.
- Renamed the `CommitPatch` type alias to `CommitSet`.
- The `..` commit selector now walks from the end boundary until it encounters
  a commit returned by the start selector. To reproduce Git's set-difference
  semantics, wrap the boundary explicitly as `ancestors(start)..end`.
- Added a `symmetric_diff` selector corresponding to Git's `A...B` three-dot
  syntax.
- Refined candidate built-in schemas in `INVENTORY.md`; removed `Bool`, the
  `BinaryLargeObject` placeholder, and the 64-bit integer types.
- Expanded the built-in schema ideas with a fuller list of value and blob
  formats to explore.
- Brainstormed an even broader range of potential schemas for long-term
  consideration.
- Added Lance, neural-network, vector-search and full-text index formats to the
  candidate blob schemas, with a note to favor memory-mapped Rust crates.
- Trimmed the candidate schemas, dropping seldom-used formats like neural
  networks, search indexes, media and font types.
- Reinstated the neural-network, HNSW and full-text index schema ideas and
  removed the tar/zip archive formats.
- Added `SocketAddr` and `RgbaColor` value types alongside a `CompressedBlob`
  wrapper, while dropping `DateYMD` and `TimeOfDay` from consideration.
- `RangeFrom` now returns `ancestors(head)` minus `ancestors(start)` while
  `..c` selects `ancestors(c)` and `..` resolves to `ancestors(head)`. The old
  `collect_range` and `first_parent` helpers were removed.
- `TimeRange` commit selector now delegates to the generic `filter` selector.
- Removed the `Completed Work` section from `INVENTORY.md`; finished tasks are
  now tracked in this changelog.
- Canonicalized epsilon closures in regular path queries and documented the
  Thompson-style automaton construction.
- Documented the currently implemented commit selectors in the book.

### Fixed
- Enforce `PREFIX_LEN <= KEY_LEN` for prefix checks in PATCH.
- Release file locks if `refresh` fails during pile branch updates to avoid lingering locks.
- Blob insertion now returns an error instead of panicking if the system clock goes backwards.
- Delay branch map updates until after branch records are written to disk, preventing divergence when writes fail.

## [0.5.2] - 2025-06-30
### Added
- Initial changelog file.
- Repository guidelines now require documenting tasks in `CHANGELOG.md`.
- Converted object-store backend to `BranchStore`/`BlobStore` API.
