# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Freeze one authorization instant in every `StoreSnapshot`, with
  `SnapshotSource::snapshot_at` as the single explicit-time construction seam.
  Collection observation and admission use that frozen instant; content-change
  masks exclude time, and authorization caches track proof-validity boundaries
  separately. Snapshot reads remain resident-only and inert; active acquisition
  belongs to the store's asynchronous ensure and maintain operations.

- Use the same `ensure` and `maintain` operations for root and derived
  collections. Roots fetch their exact admitted dependencies without a
  fictitious self-derivation or durable WANT; derived targets continue to
  publish only their own one-hop images. Remove separate acquisition and
  admission-plus-commit-list methods. `collection.read(&snapshot)` is exactly
  the resident `snapshot.collection(collection)?.view()` path.

- Make canonical `WantRequest::Blob(H)` the sole durable exact-content
  request. Repository implementations retain its exact identity, and Yard
  charges one bounded-retention slot per requested handle.

- Add independent descriptor-local READ and WRITE admission policies. Each is
  `Open` or a canonical multi-root quorum with one invocation threshold. The
  evaluator counts independently valid paths from distinct roots and adds
  exact `ACTION_READ` authorization beside `ACTION_WRITE`; delegation comes
  only from the mode signed into each path. The byte-compatible legacy
  delegation-threshold descriptor field is ignored by authorization.

- Add store-owned collection construction:
  `collection(name, policy)`, `derive(source, mapping, policy)`, and the raw
  `register_collection::<E>(descriptor)` boundary. `CollectionMapping` now
  carries associated `Source` and `Target` encodings plus its concrete mapping
  fragment. Store snapshots expose immutable typed collection observations;
  mutating ensure/maintain operations take only the target and mapping type,
  cross one explicit hop over invariant foundational support, and return a
  fresh store snapshot.

- Define foundational `Support` as exactly `Cover<SimpleArchive>` and preserve
  it unchanged across every mapping hop. `CollectionSnapshot<R, E>` pairs one
  store watermark with that support and its resident target cover. Remove the
  lifecycle facades and synthetic collection-record entity IDs; exact native
  records carry semantics and provenance, while full-width fingerprints remain
  nonsemantic storage and transport keys. `Collection::cover` names a typed
  exact coordinate without store access so durable manifests can preserve a
  cover; later admission and resolution still decide whether it is usable in a
  particular immutable snapshot.

- Make every collection member an ordinary typed `Blob<E>`.
  `CollectionEncoding` validates that blob and defines one canonical join;
  `Cover<E>` keeps the logical join total when one member hits a deterministic
  capacity boundary, so every source and derived collection is a full lattice.
  `CollectionMapping` maps blobs to blobs as a join homomorphism, while storage
  owns deterministic merge/derive sequencing and immutable dependencies.

- Add direct typed collection encodings, covers, and logical cover
  views. `CollectionEncoding` attaches canonical validation and one canonical
  member join to the member encoding; `Collection<E>` and
  `Cover<E>` retain that encoding through the public API; signed commits accept
  authored `Fragment` values in `SimpleArchive` source collections, while
  typed materialization works for non-`SimpleArchive` collections. Derived
  descriptors link a concrete mapping entity carrying its algorithm and
  concrete parameters, while exact derived lifecycles bind one
  `CollectionMapping<Source, Target>` whose law is a join homomorphism.

- Add top-level `capability`, a direct authorization kernel. One canonical
  self-contained proof encodes
  `magic16 | resource32 | root32 |`
  `N*(action16 | flags1 | validity32 | delegate32 | signature64)`. Each strict
  Ed25519 signature is last and covers the exact prefix through its delegate,
  so every signed prefix is itself a proof and paths cannot be grafted or
  reordered. Root and delegate encodings are canonical non-weak principals;
  validity remains exact over the signed `i128` nanosecond domain without
  silently saturating clock boundaries. BLAKE3 over the exact proof bytes is
  its stable identity.
  Verification takes an external trust root, expected leaf, explicit instant,
  and request, then computes the path-local action, mode, and validity meet
  without ambient or blob lookup.
- Add `CapabilityProofStore` to `MemoryRepo`, `Pile`, and `Yard` as a native
  grow-only set of self-contained proofs with deterministic enumeration and
  exact proof-ID lookup. V2 pile records use bounded canonical framing.
  Conservative collection preserves exact proof records directly; there is no
  companion blob closure or retention traversal, and proof presence alone
  grants no authority.

### Changed

- Restore bounded target-carry batching under invariant foundational support.
  Maintenance resolves collection semantics once per actionable dyadic tier
  round instead of once per individual `MERGE`, while each disjoint result is
  still stored and published immediately. Tier planning retains only indexed
  member identities and loads one input pair at a time.

- Make `YardSnapshot::blobs_diff` use the PATCH difference of the two immutable
  live-set unions, so exact-provider locator refresh is proportional to changed
  handles instead of relisting the complete Yard twice.

- Project every resident descriptor, data, metadata, and aligned attachment in
  a strictly signed, WRITE-admitted COMMIT closure into a collection-scoped
  snapshot index. Exact-content availability remains a separate,
  collection-independent property of resident blobs.

- Make store registration the sole source of typed collection values.
  `register_collection` validates a raw descriptor and returns the exact handle
  produced while storing its attachment closure; canonical root/derive builders
  are internal, `Collection::from_descriptor` and the phantom SimpleArchive
  facade are removed, and derived maintenance binds only store-issued source
  and target values while reloading and validating their lineage at use time.

- Make local `commit(collection, signer, fragment)` correct by construction:
  it stores attachments, data, and metadata before inserting the native signed
  COMMIT, without rereading a descriptor, revalidating the generated archive,
  or implicitly advertising OFFER state. Admission remains a read/sync-boundary
  decision.

- Make Yard reclaim derive its final store-scope and opaque-record safety
  decisions from one refreshed Pile state, so an opaque record appended during
  rewrite planning is refused instead of being projected away.

- Clean-cutover the unpublished Rank9 API and identities.
  `Rank9AcceleratedSuccinctArchiveBlob` is now an ABI-qualified Merkle-root
  `CollectionEncoding` whose first 32 bytes name its portable raw
  `SuccinctArchiveBlob` child. `RawToRank9AcceleratedMappingV1` maps raw members
  through ordinary `DERIVE` records. Raw Succinct and accelerated roots both
  own canonical joins. The accelerated join may consume its exact immutable raw
  union when already resident, but downstream maintenance never creates that
  upstream blob or `MERGE`. Without the dependency it retains a finer target
  cover. Cover-aware
  views pull the named child through the immutable store snapshot and validate
  the complete raw/index pair. The former `Rank9MappingV1`/`RANK9_MAPPING_V1`,
  intermediate `Rank9SidecarMappingV1*`, old blob names, and their obsolete id
  family have no compatibility aliases. The separate mapping-evidence record
  kind and store surface were removed after a scan found no live records in
  known piles; derived artifacts may be recomputed.

- Treat handles cached by typed `Blob` values returned from `BlobStoreGet` as
  trusted content identities. Collection operations retain structural and
  semantic validation at real ingress boundaries while deleting duplicate
  rehashes and post-write rereads of values produced or loaded in-process.

- Replace split Reader and revision APIs with `SnapshotSource` and one coherent
  immutable `StoreSnapshot`. Blob access, collection records, capability
  proofs are frozen together. `changes_since` defaults to
  conservative full invalidation, while `MemoryRepo`, `Pile`, and `Yard`
  compare persistent component PATCHes directly. `BLOBS` covers membership,
  metadata, and retrievability; Pile's lineage-local root-sharing comparison
  catches same-handle backing replacement while ignoring unrelated appended
  records, even though semantic PATCH equality intentionally hashes keys, not
  attached storage offsets. Yard retention planning now uses one snapshot for
  opaque-record refusal, live membership, commits, and proofs.

- Remove the obsolete `PeerRead`, `PeerStore`, and `StoreScope` repository
  surfaces and their Pile, Yard, Hybrid, Lazy, and MemoryRepo state. Historical
  framed PEER and STORE_SCOPE records decode as known inert records so old
  piles still open, while semantic rewrites omit them and continue to refuse
  unknown kinds.

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
