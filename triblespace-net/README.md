# triblespace-net

Network transport for TribleSpace piles over
[iroh](https://www.iroh.computer). `Peer<S>` wraps one local store and provides:

- content discovery and hash-verified blob transfer;
- capability-authenticated `GET_BLOB` and `CHILDREN` RPCs;
- durable weak-pin wants for lazy fetching; and
- a legacy gossip bridge that records scalar mutable-HEAD observations as
  local tracking pins.

The last item is deliberately not StrongPin replication. Signed branch
assertions and their exact `(author key, name handle)` identities do not yet
have a wire protocol or foreign-author admission boundary. `Peer<S>` forwards
local assertion-store capabilities so repository work remains usable through
the wrapper, but an appended assertion is not announced to other peers.

## Getting started

Most users enable this crate through the facade crate's `net` feature:

```toml
[dependencies]
triblespace = { version = "0.47", features = ["net"] }
```

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig, SyncDirection};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_addr],
    gossip: true,
    team_root: signing_key.verifying_key(), // single-user fallback
    self_cap: [0u8; 32],
    direction: SyncDirection::Bidirectional,
});
```

See the book's [Distributed Sync](../book/src/distributed-sync.md) and
[Capability Auth](../book/src/capability-auth.md) chapters for the exact current
boundary and team setup. The CLI surface is `trible pile net {identity,status,sync}`;
`sync` moves blobs and legacy tracking observations but never auto-signs them
into local branch assertions. Applying incoming data is fail-stop: a failed
blob write is retained as `PeerRefreshError`, and no later legacy HEAD event is
applied past it. `OP_CHILDREN` supplies store-relative traversal hints, not a
proof that some globally complete closure exists. Hint walks stream verified
partial progress while bounding retries, concurrency, provider fan-out, time,
blob count, and bytes; the current per-response transport ceilings are 256 MiB
for `GET_BLOB` and 65,536 hashes for `CHILDREN`.

## Crate layout

- `peer` — synchronous storage wrapper and local/network boundary
- `tracking` — legacy tracking-pin materialization and explicit local merge helper
- `protocol` — authenticated read-only blob wire format
- `reconcile` — durable weak-want servicing
- `identity` — node signing-key handling
- `host` / `channel` — network thread and its internal event bridge
