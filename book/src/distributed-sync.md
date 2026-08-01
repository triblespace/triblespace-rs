# Distributed Sync

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate wraps a store in an [iroh](https://www.iroh.computer/) peer. It provides
content discovery and transfer, capability-authenticated direct RPC, and an
event loop that keeps network activity behind synchronous storage traits.

This chapter describes the current migration boundary precisely. Blob transport
works. The older scalar-HEAD transport also still exists as local synchronization
machinery. **There is not yet a dedicated protocol that replicates signed branch
assertions. StrongPin branch replication is therefore incomplete and remains a
migration blocker.**

## The StrongPin Boundary

Authoritative branch state is a grow-only set of verified assertions. Its
identity is the exact pair `(author Ed25519 key, name blob handle)`; the
truncated `BranchId` is only an index prefix. Each assertion adds a commit to
that identity. Resolution derives the maximal ancestry frontier and reports
`Absent`, `TipPending`, `Partial`, or `Complete`. A complete divergent frontier
may produce a deterministic synthetic flat merge; a partial frontier exposes
only a candidate-root descriptor. Neither derived blob substitutes for signed
replicated assertions.

The network layer currently forwards `BranchAssertionStore`, `StorageFlush`,
and partial commit-DAG capabilities to its wrapped store. Consequently local
repository publication and local frontier resolution continue to work through a
`Peer<S>`. Forwarding a trait is not replication, however: assertions appended
on one peer are not yet encoded, authenticated, announced, fetched, and
admitted on another peer by an assertion-native wire protocol.

Until that protocol exists, do not interpret legacy HEAD gossip or a tracking
pin as authoritative StrongPin state, and do not claim network convergence for
branch assertions.

## What Works Today

The network stack has three useful transport pieces:

- **Blob discovery and transfer.** Content-addressed blobs are announced
  through the DHT and transferred over authenticated QUIC. A receiver verifies
  the hash, so provider identity does not change blob meaning.
- **Lazy demand and retention.** A missing blob can be recorded as a durable
  weak pin before fetching. The weak marker is local cache/retention state, not
  branch authority.
- **Legacy HEAD transport.** Gossip accepts the original 81-byte frame and
  emits an 89-byte v2 frame: tag, 16-byte remote id, 32-byte metadata-head
  hash, 32-byte publisher hint, and an 8-byte anti-deduplication nonce.
  Reachable blobs can be fetched and the observation can be materialized as a
  mutable local tracking pin.

Publisher bytes in that frame are only a bounded routing hint. Fetches try the
hint first, validate content, and then fall through to distinct DHT providers.
`OP_CHILDREN` responses are likewise store-relative hints: every accepted hash
must occur in the verified parent bytes and every fetched child must hash
correctly, but a remote response cannot prove that a global closure is
complete. If no provider answers a child-hint request the bounded walk retries;
if one accepted hint is unavailable from every route, it remains
non-authoritative and a later periodic walk may discover it. Verified fetched
blobs are emitted immediately as monotone partial progress, while the legacy
HEAD event waits for the bounded hint walk and generation check. Retry leases,
active keys, attempts, concurrent walks, provider fan-out, time, count, and
bytes are bounded independently. Persistence of the resulting event stream is
fail-stop: if a blob write fails, `Peer::refresh` remembers and returns the
error and will not apply a later HEAD event past it.

One `GET_BLOB` response is currently limited to 256 MiB and one `CHILDREN`
response to 65,536 hashes before allocation/growth. These are protocol resource
limits, not blob-encoding rules: larger local blobs are valid but require a
future chunked/streaming transport path to replicate.

Capability authentication still gates peer operations; see
[Capability Auth](capability-auth.md). Async networking remains confined to the
peer's background thread, while ordinary storage calls drain completed events.

## Tracking Pins Are Transport State

A tracking pin is a local reification of the most recently accepted legacy
remote HEAD observation. Its metadata records the remote id, publisher, and
remote name. It deliberately uses the mutable `PinStore` capability and is kept
separate from exact signed branch assertions.

Tracking pins are useful staging state:

1. legacy gossip announces a remote metadata-head hash;
2. the network follows bounded, content-bound child hints and streams the
   verified blobs it finds;
3. local tracking-pin metadata records the resolved commit; and
4. an explicit caller of `merge_tracking_into_local` can merge that commit into a workspace for the
   local author's exact branch identity, then publish a new local assertion.

Step 4 is a new local authorship act. It does not preserve or replicate the
remote author's assertion, because the legacy message did not contain that
assertion's exact name handle and signature. Tracking pins may be overwritten,
filtered, or removed as local operational state. They are neither an audit log
nor a mirror of authoritative branch state.

`pile net sync` deliberately stops after step 3. A gossip frame's publisher
field is useful for routing and separating tracking observations, but is not an
authenticated StrongPin author signature; automatic adoption would launder a
legacy observation into local authority.

This bridge is useful while the migration is underway, but it is lossy at the
branch protocol boundary. In particular, a scalar tracking pin cannot represent
an unordered multi-tip frontier, `TipPending` versus `Partial`, or two distinct
exact identities that happen to share an advisory 16-byte id.

## Missing Assertion Protocol

An assertion-native replication protocol still needs to do all of the
following:

- transfer the canonical signed assertion bytes, not a synthesized HEAD;
- identify a branch by the complete `(author key, name handle)` descriptor;
- verify signatures strictly before assertions enter the semantic snapshot;
- deduplicate semantic state by `(exact identity, commit)` while tolerating
  multiple valid signatures over the same claim;
- fetch the asserted tip metadata and missing ancestry independently, allowing
  `TipPending` and `Partial` to remain observable rather than inventing a
  complete scalar state;
- apply an explicit admission policy for foreign authors; and
- preserve set-union semantics under replay, duplication, and reordering.

Only after this exists can two peers exchange assertion sets and derive the same
maximal frontier from the same available commit DAG. Blob convergence alone is
necessary but not sufficient.

## Operational Guidance During Migration

- Use `Peer<S>` for blob movement, demand recording, and local networking.
- Treat tracking pins and legacy HEAD events as temporary internal transport
  inputs, not public branch identities.
- Use exact StrongPin identities and signed assertions for local repository
  state.
- Do not advertise the current `pile net` behavior as StrongPin branch sync.
- Do not add a shim that translates a signed assertion set back into one
  last-writer-wins HEAD; that would discard concurrency and recreate the model
  this migration removes.

The next networking milestone is therefore narrow and explicit: carry verified
branch assertions end to end, then let the existing resolver compute branch
state. The network should transport facts; ancestry and frontier semantics
remain in the repository layer.
