# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Removed the unused mutable-manifest range replacement helpers and monotone
  commit-batch guard. Range records retain their immutable open-fact model;
  pool cover selection derives freshness from the authoritative frontier.
- Added deterministic `select_range_cover` with source-data residuals for
  merged grow-only rollup pools. Selection is keyed by standalone record blob
  handle, excludes off-frontier ranges, isolates invalid candidates, and uses
  canonical commit metadata rather than accepting arbitrary parent facts.
- Added a typed, grow-only `RollupPinDescriptor` keyed by source branch and
  index recipe. Each signed assertion pairs a hard core-only range-record
  value with one complete unowned artifact-node handle in its opaque label.
  This keeps coverage metadata durable without turning every historical
  derived payload into permanent weak-pin demand; equal-range nodes remain
  atomic alternatives rather than being fact-unioned.
- Hard retention is now an explicit `StrongPinDescriptor` decorator rather
  than a branch-special case in Yard. The canonical V1 wrapper contains an
  exact inner descriptor handle; authentic assertions sharing its outer pin
  retain the outer plus the locally present closure of the inner descriptor
  and every distinct value. `BranchPinDescriptor` is a clean-break aligned V2
  inner descriptor, and repository publication flushes both descriptor layers
  before appending the newly bound assertion.
- Removed the scalar `IndexHome` catalog and its `PinStore` dependency.
  Derived-index snapshots are exact, immutable `SimpleArchive` values stored
  and loaded by content handle. Standalone loading rejects arbitrary empty
  archives, branch wrappers, unrelated facts, and bundled recipes; attachment
  rechecks the runtime recipe identity. Source freshness is now compared with
  an authoritative head supplied by the caller rather than self-claimed branch
  metadata inside the cache blob.
- Removed `AsyncPinStore`, its adapter implementations, and
  `ObjectStoreRemote`'s legacy `pins/` CAS namespace. The remote backend now
  stores content-addressed blobs only; it does not pretend that object-store
  listing and conditional PUT provide an asserted-state ledger.
- Lazy demand is now an author-scoped `PinAssertion` G-set rather than mutable
  weak-pin state. `Lazy::new(store, SigningKey)` requires the author capability,
  `WantStore` exposes only that author's wants, and a miss crosses the durable
  assertion-append boundary without a second flush. Raw Pile and Yard reads are
  observational; old weak pin/unpin records remain decodable for forensics but
  are ignored semantically and are not migrated automatically. Yard retains a
  canonical global prefix through `YardConfig::want_budget` (renamed from
  `weak_budget`) before presence filtering. `WantCachePolicySource` exposes
  only that artifact-local capacity to service layers; selection remains one
  fixed raw-handle-ordered global prefix before author or presence filtering,
  giving bounded collection and reconciliation a stable fixed point. The full
  authentic set supplies hard-reachability cut points: absent values remain
  durable demand, while present values are retained only through the bounded
  soft prefix instead of silently inheriting hard retention.
- Generic `PinAssertionStore` provides one grow-only signed envelope for every
  pin kind. Its full-width `(author, descriptor)` identity, value, and opaque
  label are kind-neutral; `BranchPinDescriptor` and `BranchRank` supply the
  branch-specific commit and causal-order semantics without a separate branch
  assertion store. Async adapters forward this generic capability plus
  `PartialCommitDag` and truthful `StorageFlush`; `HybridStore` composes blobs
  with a separate asserted-pin store, and `Lazy` preserves assertion authority
  while turning only genuine missing commit metadata into durable wants.
- `MemoryRepo::branches` is renamed to `pins`; mutable cells are no longer
  presented as content-branch authority even in test storage.
- The public raw-pile vocabulary now exposes historical mutable records as
  `PileRecordContent::{Pin, PinTombstone}` and `PilePinStoreIter`. Existing wire
  markers and bytes remain unchanged; only the misleading public names are
  removed.

## [0.41.4] - 2026-05-17

Lock-step bump alongside the trailing-dot-leak +
connection-reuse fixes in `triblespace-net` / `trible`. No
source changes in `triblespace-core`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.3] - 2026-05-17

Lock-step bump alongside the trailing-dot relay-URL fix in
`triblespace-net` / `trible`. No source changes in
`triblespace-core`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.2] - 2026-05-17

Lock-step bump alongside the address-symmetry work in
`triblespace-net` / `trible`. No source changes in
`triblespace-core`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.1] - 2026-05-17

Lock-step bump alongside the EndpointTicket-everywhere work
in `triblespace-net` / `trible`. No source changes in
`triblespace-core`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.41.0] - 2026-05-16

Lock-step bump alongside the iroh 0.98 family upgrade in
`triblespace-net`. No source changes in `triblespace-core`.
See the workspace [`../CHANGELOG.md`](../CHANGELOG.md) for
the full release notes.

## [0.39.0] - 2026-05-11

The canonical-attribute-id + bounded-path-estimation release.
See the workspace [`../CHANGELOG.md`](../CHANGELOG.md) for the
full release notes on dynamic-name attribute id derivation,
the IRI BlobEncoding, `metadata::iri`, `Attribute::from_iri`, the
`MemoryBlobStore::union` structural merge, and the `Workspace`
`local_blobs → staged` rename.

### Path-query: bounded-depth closure estimation
- **`estimate_from`'s closure-fallback no longer full-materialises**
  the result set
  (`triblespace-core/src/query/regularpathconstraint.rs`). The
  previous fallback ran `eval_from(set, body, start).len()` —
  paying the full cost of computing the closure just to measure
  its size. The new `bounded_eval_from` helper caps closure BFS
  at `RPQ_ESTIMATE_DEPTH = 5` levels, matching Karalis et al.
  ESWC 2024 §4.3's "default estimation": bounded depth →
  bounded estimate cost, sufficient for variable ordering.
  Non-closure expressions don't consume depth; the bound only
  fires on Plus/Star iteration steps. Nested closures multiply
  (`Plus(Plus(q))` runs the inner Plus to depth 5 for each of
  the outer's 5 steps — `O(depth^k)` for closure-nesting
  depth `k`), which the doc comment flags. Shallow estimation
  (the constant-time per-attribute count from the segmented
  index) was already in place; this commit closes the remaining
  gap where shallow doesn't apply.

## [0.38.0] - 2026-05-07

Lock-step bump alongside the team-rooted-gossip release in
`triblespace-net` / `trible`. No source changes in
`triblespace-core`. See the workspace
[`../CHANGELOG.md`](../CHANGELOG.md) for the full release notes.

## [0.37.0] - 2026-05-06

First per-crate CHANGELOG. Earlier `triblespace-core` releases
are documented at the workspace level in
[`../CHANGELOG.md`](../CHANGELOG.md).

### Added
- **`PathOp::Optional` (`(p)?`) primitive** in the path-query
  language. `Optional(p)` matches zero-or-one applications of
  `p`; semantically `Union(Identity, p)` but recognised inline
  so the zero-step branch reuses the bound start node directly
  instead of materialising every node as an `Identity`
  candidate. Same shape as the `Star` arm but with the zero-
  step alone (no transitive frontier). Plus a `from_postfix`-
  time normalisation pass that distributes `Optional` and
  `Union` out of `Concat` via the standard rewrites
  (`a / b? ↔ a | (a / b)`, `(a | b) / c ↔ (a / c) | (b / c)`,
  etc.) — without it, the typical WDBench shape
  `Concat(Attr, Optional(Attr))` (`p / q?`) would hit the
  `build_constraint` `unreachable!()` arm. Macro syntax in
  `path!` (`(p)?`) is the follow-up; until then callers
  construct `PathOp::Optional` postfix-style via
  `RegularPathConstraint::new`. Two proptests cover the
  standalone `(p)?` boundary case and the `p / p?` Concat-
  with-Optional case post-normalisation.
- **`PathOp::Inverse` (`^p`) primitive** in the path-query
  language. `^attr` reverses the direction of an attribute
  edge (VAE-index lookup yielding entity bytes, mirroring the
  existing forward `eval_attr` / EAV-index path). Compound
  expressions push down via the standard reversal rewrites
  (`^(a/b) ↔ ^b/^a`, `^(a+) ↔ (^a)+`); double negation
  (`^^a → a`) cancels at `from_postfix`-time. Macro syntax in
  `path!` (`^p`) is the follow-up; until then callers
  construct `PathOp::Inverse` postfix-style via
  `RegularPathConstraint::new`. Two proptests cover
  standalone `^link` and `(^p / p)+` (mid-path inverse inside
  a Plus loop).
- **`Universe::search_range(min, max) -> Range<usize>`**, plus
  the underlying `search_lower(v)` / `search_upper(v)`
  primitives. `O(log n)` half-open code range over a monotonic
  universe; default impls fall through to a binary search via
  `Universe::access`. Implementations with a flat sorted slice
  override to skip the virtual-call overhead.
- **`SuccinctArchive::value_in_range`** constraint exploits
  the new universe primitive: `O(log n + K)` proposals over
  range-bounded values, where `K` is the number of distinct
  in-range codes that actually appear on the indexed axis.
  Composable with `pattern!` / `find!` / `and!`. Combined with
  `enumerate_in_range` (the bounded variant of
  `enumerate_domain`), it gives the engine a real range-query
  primitive without scanning the full value column.
- **`repo::capability` runnable doctests** on every primary
  public function: `build_capability`, `verify_chain`,
  `build_revocation`, `extract_revocation_pairs`,
  `VerifiedCapability` (covering `permissions`,
  `granted_branches`, `grants_read`, `grants_read_on`).

### Changed
- **`SuccinctArchive`'s value-axis enumeration** routes
  range-bounded queries through `Universe::search_range`
  rather than enumerating the full domain and post-filtering.
  Same result; `O(log n + K)` instead of `O(n)`.
- **Workspace doc warnings cleaned** — 9 stale intra-doc-link
  warnings in `Universe` trait method docs and the
  `succinctarchive` module fixed (`[Self::search]`,
  `[Self::access]`, `[Self::search_lower]`,
  `[Self::enumerate_domain]` etc.). `cargo doc -p
  triblespace-core --no-deps` is now warning-free.
