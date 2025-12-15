# Documentation Improvement Ideas

This chapter is a roadmap for the next iteration of the book. Each subsection
summarises a gap we discovered while reviewing the crate and outlines the
minimal content that would help readers apply the feature in practice. When you
pick one of these items up, try to produce a runnable example (or at least
executable pseudocode) so the section teaches a concrete workflow rather than a
theory sketch.

## High-priority topics

The following themes unblock common deployment or operational scenarios and
should be tackled first when planning documentation work:

### Remote object stores
`repo::objectstore::ObjectStoreRemote::with_url` wires the repository into
[`object_store`](https://docs.rs/object_store/latest/object_store/) services such
as S3, local filesystems or Azure storage. The future chapter should walk
through credential configuration, namespace selection, and pairing the remote
backend with other stores (for example via `HybridStore`). It also needs to call
out how branch updates rely on `PutMode::Update`/`UpdateVersion` retries, how
conflicts bubble up to callers, and how listings stream through
`BlockingIter` so readers know what consistency guarantees to expect. 【F:src/repo/objectstore.rs†L108-L316】

### Hybrid storage recipes
`repo::hybridstore::HybridStore` mixes a blob store with a separate branch
store. Documenting a few reference layouts—remote blobs with local branches,
piles with in-memory branches, or even two-tier caches—will help teams evaluate
trade-offs quickly. 【F:src/repo/hybridstore.rs†L1-L86】

### Signature verification
Both `repo::commit::verify` and `repo::branch::verify` expose helpers for
validating signed metadata before accepting remote history. A hands-on example
should explain when to perform verification, how to surface failures to callers,
and which key material needs to be distributed between collaborators. 【F:src/repo/commit.rs†L84-L122】 【F:src/repo/branch.rs†L95-L136】

### Repository migration helpers
`repo::transfer` rewrites whichever handles you feed it and returns the old and
new identifiers so callers can update references. A migration recipe could show
how to collect handles from `BlobStoreList::blobs()` for full copies or from
`reachable` when only live data should be duplicated. Highlight how the helper
fits into a scripted maintenance window. 【F:src/repo.rs†L394-L516】

### Conservative GC tooling
The garbage-collection chapter covers the high-level approach, but it should
also reference concrete APIs such as `repo::reachable`, `repo::transfer`, and
`MemoryBlobStore::keep`. Describing how to compute and retain the reachable set
in code makes it easier to embed the GC workflow into automated jobs. 【F:src/repo.rs†L394-L516】 【F:src/blob/memoryblobstore.rs†L169-L210】

## Emerging capabilities

These topics are less urgent but still deserve coverage so that readers can
reuse advanced building blocks without digging through source code.

### Succinct archive indexes
`blob::schemas::succinctarchive::SuccinctArchive` converts a `TribleSet` into
compressed wavelet matrices, exposes helpers such as `distinct_in` and
`enumerate_in`, implements `TriblePattern`, and serialises via ordered,
compressed or cached `Universe` implementations. A dedicated section should walk
through building an archive from a set, choosing a universe, storing it as a
blob, and querying it directly through `SuccinctArchiveConstraint` so readers
can reuse the on-disk index without round-tripping through `TribleSet`
conversions. 【F:src/blob/schemas/succinctarchive.rs†L100-L529】 【F:src/blob/schemas/succinctarchive/universe.rs†L16-L265】 【F:src/blob/schemas/succinctarchive/succinctarchiveconstraint.rs†L9-L200】

### Extensible path engines
Regular path queries run through `RegularPathConstraint`, which delegates edge
checks to the `PathEngine` trait. The book should document how the built-in
`ThompsonEngine` constructs NFAs from a `TribleSet` and demonstrate how to plug
in alternative engines backed by other graph stores so readers can extend the
regex-based traversal beyond in-memory datasets. 【F:src/query/regularpathconstraint.rs†L1-L200】

## How to keep this list fresh

Treat these notes as a living backlog. Whenever a new subsystem lands, ask
yourself whether it needs a discoverability guide, a tutorial or a troubleshooting
section. Update this chapter with the gaps you observe, and link to the relevant
modules so future contributors can jump straight into the implementation.
