# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- 0.6.0 release preparation checklist in `INVENTORY.md` detailing actionable
  subtasks for the remaining blockers and polish items.
- Formal verification roadmap outlining Kani, Miri, fuzzing, and simulation
  testing plans in the book.
- Expanded the roadmap with an explicit invariant catalogue, spelling out the
  PATCH/ByteTable invariants exercised by `Branch::modify_child`, and synchronised
  follow-up tasks in `INVENTORY.md`.
- `_?ident` scoped variables for `pattern!` and `pattern_changes!`, enabling
  fresh bindings without declaring them in `find!` heads, along with
  documentation and tests.
- Documented repository storage backends and added a book page tracking future
  documentation improvements.
- Expanded the documentation backlog with notes on remote object-store conflict
  handling, succinct archive indexes, and extending regular path engines.
- Glossary chapter in the book for quick reference to core terminology.
- Expanded the Identifiers chapter with a `local_ids` + `IdOwner` workflow
  example showing how to borrow freshly minted IDs in queries.
- `nth_ancestor` commit selector corresponding to Git's `A~N` syntax and
  documentation updates.
- `parents` commit selector corresponding to Git's `A^@` syntax.
- `INVENTORY.md` file and instructions for recording future work.
- README now links to the corresponding chapters on https://triblespace.github.io/tribles-rust.
- `Constraint::influence` method for identifying dependent variables.
- Documentation and examples for the repository API.
- Book section showing how to stage and fetch workspace blobs with `Workspace::put`
  and `Workspace::get`.
- Guidance on integrating custom constraints with external data sources in the book.
- Garbage-collection chapter now shows how `BlobStoreList`, `reachable`, and
  `transfer` work together to enumerate and traverse blobs in practice.
- Remote store workflow example in the book showing how to open
  `ObjectStoreRemote` repositories and clarifying that no explicit close is
  required for remote backends.
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

### Changed
- Expanded the Pile Format chapter with a restore-first operational workflow,
  corrected usage example showing a restore-after-open startup without an
  explicit refresh, and detailed record field breakdowns.
- Clarified that `Pile::restore` already applies intact records before
  truncating and that reader/branch helpers refresh automatically, so manual
  refreshes are only needed when scanning between operations.
- Normalized the Descriptive Typing chapter to use consistent Markdown headings
  and remove unused front matter.
- Clarified `PATCH::iter_ordered` and `PATCHOrderedIterator` documentation to
  describe the full tree-order traversal without a prefix filter and point to
  the prefix iterator for filtered traversal.
- Documented the `path!`, `attributes!`, and `pattern_changes!` procedural
  macros in the `tribles-macros` crate overview.
- Reframed commit range selectors so `start..end` walks from the end selector
  until encountering a commit yielded by the start selector, reducing
  redundant ancestor exploration and making the traversal cost explicit.
- Query Engine chapter now directs readers to the crate-level `pattern!` and
  `entity!` macros and shows how to import them via the prelude.
- Removed the outdated note that parentheses "force" literals in the getting
  started guide now that the macros rely on regular Rust expression syntax for
  literal detection.
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
- Getting started guide now highlights the need to close pile-backed repositories so callers can handle flush errors explicitly.
- README example now inlines the shared `tribles::examples::literature` namespace so the getting started walkthrough and crate examples stay aligned without depending on internal modules.
- README walkthrough and regression test keep the namespace name `literature` to match the shared example module.
- `with_sorted_dedup` now accepts iterators so compressed universes can build domains without materializing values.
- `SuccinctArchiveMeta` now accepts the domain's serialized metadata type,
  removing its hardcoded `SectionHandle<RawValue>` dependency.
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
  `ignore!` skips variables so their constraints aren't checked, enabling
  existential or don't-care matches.
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

