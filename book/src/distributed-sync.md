# Distributed Sync

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate wraps a store in an [iroh](https://www.iroh.computer/) peer. It provides
content discovery and transfer, capability-authenticated direct reads, and a
lazy reconciler for durable weak-pin wants.

This is intentionally **blob sync, not StrongPin branch sync**. The old scalar
mutable-HEAD gossip/tracking bridge and `OP_CHILDREN` traversal RPC are gone.
There is not yet a protocol that transports signed branch assertions, so two
peers do not converge their authoritative branch state merely by running
`pile net sync`.

## The StrongPin Boundary

Authoritative branch state is a grow-only set of signed assertion witnesses. Its
identity is the exact pair `(author Ed25519 key, name blob handle)`; the
truncated `BranchId` is only an index prefix. Each assertion adds a commit to
that identity. Resolution derives the maximal ancestry frontier and reports
`Absent`, `TipPending`, `Partial`, or `Complete`. A complete divergent frontier
may produce a deterministic synthetic flat merge; a partial frontier exposes
only a candidate-root descriptor. Neither derived blob substitutes for the
signed assertions from which it was derived.

Pile replay structurally indexes those witnesses without verifying every
historical signature. The resolver may use local ancestry optimistically, but
before it exposes a candidate as a tip or turns missing ancestry into demand it
verifies a witness for every surviving `(exact identity, commit)` claim group.
An all-invalid group is removed and domination is recomputed. Verification is
memoized, so a long linear history normally verifies only its surviving tip;
invalid witnesses never authorize a fetch.

The network wrapper forwards `BranchAssertionStore`, `StorageFlush`, and
partial commit-DAG capabilities to its wrapped store. Local repository
publication and frontier resolution therefore continue to work through a
`Peer<S>`. Forwarding storage traits is not replication: an assertion appended
on one peer is not encoded, announced, fetched, verified, or admitted on
another peer by the current network protocol.

## What Works Today

The current stack moves content in three complementary steps:

- **Discovery.** A peer announces the hashes of locally available blobs through
  the DHT. Announcements are provider hints, not branch authority.
- **Scoped transfer.** A client dials a provider over pile-sync v5, presents a
  capability with mandatory first-stream `OP_AUTH`, and requests one known hash
  with `OP_GET_BLOB`. The receiver verifies the content hash before accepting
  the bytes.
- **Lazy demand and retention.** A missing blob is recorded and flushed as a
  durable weak-pin want before a fetch begins. The reconciler retries provider
  lookup and transfer; an unavailable blob remains pending rather than losing
  the demand. `--no-lazy` disables this reconciler.

Pile-sync v5 (`/triblespace/pile-sync/5`) has no branch enumeration, HEAD,
child-list, or remote-write operation. It serves only `OP_AUTH` and
`OP_GET_BLOB`. In particular, the network does not infer a transitive closure
from arbitrary blob bytes. Higher layers request the exact content handles
their typed data names, and a missing dependency becomes another explicit
weak-pin want.

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

An assertion-native replication protocol still needs to do all of the
following:

- transfer canonical signed assertion bytes, not synthesize a scalar HEAD;
- identify a branch by the complete `(author key, name handle)` descriptor;
- verify a surviving witness strictly before any asserted tip or fetch demand
  enters the semantic result;
- deduplicate semantic state by `(exact identity, commit)` while tolerating
  multiple valid signatures over the same claim;
- fetch asserted tip metadata and missing ancestry independently, preserving
  `TipPending` and `Partial` instead of inventing a complete scalar state;
- apply an explicit admission policy for foreign authors; and
- preserve set-union semantics under replay, duplication, and reordering.

Only after this exists can two peers exchange assertion sets and derive the
same maximal frontier from the same available commit DAG. DHT/blob convergence
is necessary but not sufficient.

## Operational Guidance During Migration

- Use `Peer<S>` for DHT announcement, authenticated blob movement, durable
  demand recording, and lazy retrieval.
- Use exact StrongPin identities and signed assertions for local repository
  state.
- Do not advertise current `pile net sync` behavior as StrongPin branch sync.
- Do not translate an assertion set back into one last-writer-wins HEAD; that
  would discard concurrency and recreate the model this migration removes.

The next branch-networking milestone is narrow and explicit: carry verified
branch assertions end to end, then let the existing resolver compute branch
state. The network should transport facts; ancestry and frontier semantics
remain in the repository layer.
