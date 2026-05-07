# triblespace-net

Distributed sync for triblespace piles over [iroh](https://www.iroh.computer):
gossip for branch HEAD announcements, a DHT for content discovery, direct
QUIC for bulk transfer. The user-visible surface is a single wrapper type,
`Peer<S>`, that makes any triblespace store also a node on a distributed
graph without changing how the storage traits look from outside.

## Getting started

Most users should enable `triblespace-net` through the facade crate's
`net` feature rather than depending on this crate directly:

```toml
[dependencies]
triblespace = { version = "0.35", features = ["net"] }
```

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_id],
    gossip: true,
    team_root: signing_key.verifying_key(), // single-user fallback
    revoked: HashSet::new(),
    self_cap: [0u8; 32],
});
// From here it's just a triblespace store — commit, push, pull, query.
```

The full mental model, transport details, `track` / `fetch` primitives,
merge-flow, timestamp ordering, and CLI surface (`trible pile net
{identity, sync, pull}`) live in the
[Distributed Sync](https://docs.rs/triblespace/latest/triblespace/) book
chapter.

## Crate layout

- `peer` — `Peer<S>` wrapper + `pull_branch` / `track` / `fetch` /
  `list_remote_branches` / `head_of_remote` / `resolve_branch_name`
- `tracking` — tracking branch management + `merge_tracking_into_local`
- `protocol` — wire-format types and ALPN
- `identity` — `load_or_create_key` and signing-key handling

The `host` and `channel` modules are private implementation details of
`Peer` — the network thread spawned per Peer and the mpsc channels that
bridge it to the sync storage layer.
