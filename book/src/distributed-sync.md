# Distributed Sync

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate wraps a store in an [iroh](https://www.iroh.computer/) peer. It provides
content discovery and transfer, capability-authenticated direct reads, and a
lazy reconciler for durable signed asserted wants.

This is intentionally **blob sync, not asserted-pin sync**. The old scalar
mutable-HEAD gossip/tracking bridge and `OP_CHILDREN` traversal RPC are gone.
There is not yet a protocol that transports generic pin assertions, so two
peers do not converge their branch pins merely by running
`pile net sync`.

## The Asserted-Pin Boundary

`PinAssertionStore` carries a grow-only set of generic signed witnesses. Its
exact identity is `(author Ed25519 key, descriptor blob handle)`, indexed by a
full-width digest and rechecked exactly. The envelope's value and 32-byte label
are opaque until a typed adapter recognizes the descriptor.

For the branch kind, a locally present canonical `BranchPinDescriptor` maps the
generic pin handle back to a branch-name handle. Its value is a commit and its
label is a causally monotone `BranchRank`. The rank may skip an ancestry check
that cannot succeed; it never proves domination or removes a claim. Resolution
derives the maximal ancestry frontier and reports
`Absent`, `TipPending`, `Partial`, or `Complete`. A complete divergent frontier
may produce a deterministic synthetic flat merge; a partial frontier exposes
only a candidate-root descriptor. Neither derived blob substitutes for the
generic witnesses from which it was derived.

Pile replay structurally indexes those witnesses without verifying every
historical signature. The resolver may use local ancestry optimistically, but
before it exposes a candidate as a tip or turns missing ancestry into demand it
verifies a witness for every surviving `(exact identity, commit)` claim group.
An all-invalid group is removed and domination is recomputed. Verification is
memoized, so a long linear history normally verifies only its surviving tip;
invalid witnesses never authorize a fetch.

The network wrapper forwards `PinAssertionStore`, `StorageFlush`, and
partial commit-DAG capabilities to its wrapped store. Local repository
publication and frontier resolution therefore continue to work through a
`Peer<S>`. Forwarding storage traits is not replication: an assertion appended
on one peer is not encoded, announced, fetched, verified, or admitted on
another peer by the current network protocol.

Wants are another typed view over the same assertion set. Every author uses the
fixed `WantPinDescriptor`, while the author key keeps principals distinct. The
asserted value is the exact wanted blob handle and the label is fixed canonical
padding, not an ordering relation. Consequently, each author's wants form a
grow-only set: duplicate assertions collapse, satisfaction merely makes a want
inert, and there is no unpin or tombstone.

## What Works Today

The current stack moves content in three complementary steps:

- **Discovery.** A peer announces the hashes of locally available blobs through
  the DHT. Announcements are provider hints, not branch authority.
- **Scoped transfer.** A client dials a provider over pile-sync v5, presents a
  capability with mandatory first-stream `OP_AUTH`, and requests one known hash
  with `OP_GET_BLOB`. The receiver verifies the content hash before accepting
  the bytes.
- **Lazy demand and retention.** A missing blob is recorded as a durable
  assertion before a fetch begins. `Lazy` and `Peer` own a signing key, so both
  appending and reading wants are scoped to that configured author. Assertion
  append is already durable; it needs no second flush. The reconciler retries
  provider lookup and transfer for that same author's share of the store's
  canonical global want-cache prefix. Capacity is applied across authentic
  values from every author before the local-author intersection, matching
  `Yard` retention exactly; over-budget demand remains durable without entering
  a fetch/evict loop. An unavailable selected blob remains pending rather than
  losing the demand. `--no-lazy` disables this reconciler.

Pile-sync v5 (`/triblespace/pile-sync/5`) has no branch enumeration, HEAD,
child-list, or remote-write operation. It serves only `OP_AUTH` and
`OP_GET_BLOB`. In particular, the network does not infer a transitive closure
from arbitrary blob bytes. Higher layers request the exact content handles
their typed data names, and a missing dependency becomes another explicit
asserted want. Raw `Pile` and `Yard` reads remain observational: only a wrapper
that owns an authoring key records demand.

One `GET_BLOB` response is limited to 256 MiB at both endpoints. The server
checks the shared store view before making an owned response copy, and the
receiver checks the declared length before allocating. This is a transport
limit, not a blob-encoding rule: larger local blobs remain valid but need a
future chunked transport path.

Serving snapshots fail closed. If the peer cannot rebuild a complete snapshot
after local state changes, it clears the previous serving view; it never keeps
authorizing reads against stale roots or stale blob membership.

Capability authentication, proof bootstrap, connection limits, and join
delivery policy are described in [Capability Auth](capability-auth.md). Async
networking remains confined to the peer's background thread, while ordinary
storage calls drain completed bounded events.

## Missing Assertion Protocol

An asserted-pin replication protocol still needs to do all of the
following:

- transfer canonical generic envelope bytes, not synthesize a scalar HEAD;
- identify every pin by the complete `(author key, descriptor handle)` pair and
  preserve descriptors for kinds the receiver does not understand;
- recognize a branch only through the canonical `BranchPinDescriptor`, whose
  content names the human-facing branch name;
- verify a surviving witness strictly before any asserted tip or fetch demand
  enters the semantic result;
- deduplicate branch state by `(exact pin identity, commit)` while tolerating
  multiple valid signatures over the same claim;
- fetch asserted tip metadata and missing ancestry independently, preserving
  `TipPending` and `Partial` instead of inventing a complete scalar state;
- apply an explicit admission policy for foreign authors and pin kinds; and
- preserve set-union semantics under replay, duplication, and reordering.

Only after this exists can two peers exchange assertion sets and derive the
same maximal frontier from the same available commit DAG. DHT/blob convergence
is necessary but not sufficient.

## Operational Guidance During Migration

- Use `Peer<S>` for DHT announcement, authenticated blob movement, durable
  author-scoped demand recording, and lazy retrieval.
- Use exact typed branch descriptors over generic asserted pins for local
  repository state.
- Do not advertise current `pile net sync` behavior as asserted-pin or branch
  sync.
- Do not translate an assertion set back into one last-writer-wins HEAD; that
  would discard concurrency and recreate the model this migration removes.

The next networking milestone is narrow and explicit: carry verified generic
pin assertions and their descriptor blobs end to end, then let typed adapters
such as the branch resolver compute meaning. The network should transport
facts; rank, ancestry, and frontier semantics remain in the branch layer.
