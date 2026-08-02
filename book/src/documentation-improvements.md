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
`repo::objectstore::ObjectStoreRemote::with_url` wires blob storage and the
content-addressed `blobs/` namespace into
[`object_store`](https://docs.rs/object_store/latest/object_store/) services such
as S3, local filesystems or Azure storage. It deliberately does not implement
`PinAssertionStore`: the generic `ObjectStore::list` contract is not a coherent
point-in-time snapshot, and the local-filesystem backend does not expose the
file and directory durability barriers required by a grow-only assertion
ledger.
The future chapter should cover credentials, namespace selection, hash
validation on reads, and the additional
backend-specific durability capability required before remote blobs can sit on
a repository publication path. A future remote assertion ledger needs an
explicit immutable-snapshot protocol and truthful durability boundary; a
LIST-plus-GET shim is not sufficient.

### Hybrid storage recipes
`repo::hybridstore::HybridStore` mixes a blob store with a separate generic
asserted-pin store. Documenting a few reference layouts—durable piles with a
separate assertion ledger, piles with in-memory assertions for tests, or
two-tier blob caches—will help teams evaluate trade-offs quickly. Remote blob
storage becomes such a layout only once its adapter truthfully exposes the
pre-publication flush boundary. Assertion append is already durable by its own
trait contract.

### Signature verification
`repo::commit::verify` validates signed commit metadata, while
`PinAssertion::decode_verified` is the public constructor for generic envelope
bytes received from a peer. A hands-on example should distinguish
cryptographic validity from authorization: signature verification admits no
foreign assertion or descriptor kind by itself, and the remote ingest boundary
must still enforce exact identity, kind, and resource policies. Branch handling
then separately unwraps `StrongPinDescriptor`, loads `BranchPinDescriptor`, and
interprets the opaque label as `BranchRank`; the generic layer must not do
either.

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
`blob::encodings::succinctarchive::SuccinctArchive` converts a `TribleSet` into
compressed wavelet matrices, exposes helpers such as `distinct_in` and
`enumerate_in`, implements `TriblePattern`, and serialises via ordered,
compressed or cached `Universe` implementations. A dedicated section should walk
through building an archive from a set, choosing a universe, storing it as a
blob, and querying it directly through `SuccinctArchiveConstraint` so readers
can reuse the on-disk index without round-tripping through `TribleSet`
conversions. 【F:src/blob/encodings/succinctarchive.rs†L100-L529】 【F:src/blob/encodings/succinctarchive/universe.rs†L16-L265】 【F:src/blob/encodings/succinctarchive/succinctarchiveconstraint.rs†L9-L200】

## How to keep this list fresh

Treat these notes as a living backlog. Whenever a new subsystem lands, ask
yourself whether it needs a discoverability guide, a tutorial or a troubleshooting
section. Update this chapter with the gaps you observe, and link to the relevant
modules so future contributors can jump straight into the implementation.
