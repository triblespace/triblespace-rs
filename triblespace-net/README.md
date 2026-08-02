# triblespace-net

Network transport for TribleSpace piles over
[iroh](https://www.iroh.computer). `Peer<S>` wraps one local store and provides:

- DHT content announcements and hash-verified blob transfer;
- capability-authenticated, scope-gated `GET_BLOB`;
- durable weak-pin wants for lazy fetching; and
- the capability request, delivery, renewal, and proof-bootstrap channel.

The network does **not** replicate StrongPin branch assertions yet. Appending a
signed assertion to one peer does not announce or admit that assertion on
another peer. The former scalar mutable-HEAD gossip/tracking bridge and its
`OP_CHILDREN` traversal RPC were deleted rather than carried into the StrongPin
model.

## Capability model

The offline team root signs exactly one explicit, non-expiring
`FounderAnchor`: a tagged root-to-founder declaration with maximum scope and no
`expires_at`. The anchor is proof/rotation authority only and is rejected if
presented as an authentication leaf. Every credential used by `OP_AUTH` is a
separate finite operational capability. Founder renewals are siblings directly
under the retained anchor, so expiry can advance without an ever-growing proof
or another root signature.

The durable per-team credential pin is authoritative. It atomically retains the
current finite cap/sig pair plus the founder-anchor sig when this node is the
founder; process configuration cannot replace an existing pin. If the finite
credential expires but its entire proof remains exact, startup is recovery-only:
no outbound `OP_AUTH` or ordinary authorized work occurs until the founder
locally issues a fresh sibling or an ordinary member accepts its authorized
renewal delivery. Corrupt, malformed, wrong-root, or wrong-subject credentials
still fail startup.

The outbound first-delivery path is crash-recoverable across its separate
request and team pins. After the verified proof bundle is durable, the exact
pending request is CAS-claimed as an activation journal before the credential
pin changes; startup can finish that activation, and a concurrent replacement
request cannot be consumed by a stale delivery. Founder renewal likewise
reconciles the unique non-retracted self-policy keyed by verified
`(subject, scope)` with the active credential before issuing another sibling.
That policy uses the verified chain-effective deadline. Its local delivery
marker journals host publication, so a durable credential winner is placed in
the coherent serving snapshot and installed for outbound auth before the
marker is completed.

## Getting started

Most users enable this crate through the facade crate's `net` feature:

```toml
[dependencies]
triblespace = { version = "0.47", features = ["net"] }
```

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_addr],
    team_root: team_root_pubkey,
    self_cap: bootstrap_cap_sig_handle, // one-time seed if no durable pin exists
});
```

`self_cap = [0u8; 32]` is an explicit server-only sentinel, not a synthesized
root-issued finite credential. It leaves inbound serving and recovery delivery
available while outbound authenticated operations remain disabled.

See the book's [Distributed Sync](../book/src/distributed-sync.md) and
[Capability Auth](../book/src/capability-auth.md) chapters for the exact current
boundary and team setup. The CLI surface is
`trible pile net {identity,status,sync}`. `sync` announces local content and
services durable weak-pin wants; `--no-lazy` disables the reconciler. It does
not synthesize or exchange branch state.

Pile-sync protocol v5 (`/triblespace/pile-sync/5`) deliberately has only two
operations: mandatory first-stream `OP_AUTH`, followed by scope-gated
`OP_GET_BLOB`. `GET_BLOB` enforces its 256 MiB transport envelope at both
endpoints. Snapshot construction is fail-closed: if the store cannot produce a
complete serving view, the peer clears the old view instead of continuing to
serve stale authorization state.

The separate auth-handshake v2 ALPN handles one-shot capability requests,
deliveries, and exact proof-member fetches. Proof locators are not secrets: a
server returns a requested member only after its own complete local
verification proves that the member was touched by the named leaf chain. Cold
`OP_AUTH` proof members remain connection-local. A delivered leaf pair and its
verified parent-proof bundle cross into the policy thread as one bounded event;
the active credential is published only after the complete selected bundle is
stored.

Capability proof verification computes the earliest operational expiry in the
chain; the non-expiring founder anchor does not shorten it. Ordinary auth
requires that deadline to be live. The separate recovery verifier checks the
same signatures, anchor, exact proof shape, delegation links, scopes, depth,
and interval encodings but permits startup to classify an otherwise-valid
expired pin without authorizing it.

Authenticated connections and post-auth streams are globally bounded, and a
subject may hold at most one live inbound pile-sync connection. Capability
request, delivery, and confirmation queues are bounded as well. A wire
`STATUS_OK` for request or delivery acknowledges queue admission, not durable
policy acceptance. Stream tails retain their subject and global connection
leases; an independent monotonic expiry deadline closes idle authority, and
each operation authorizes only after its complete frame has arrived.

## Crate layout

- `peer` — synchronous storage wrapper, durable policy boundary, and lazy reads
- `protocol` — authenticated v5 blob wire format
- `handshake` — v2 request, delivery, and exact proof-bootstrap protocol
- `policy` — local pending requests, renewal state, and active credentials
- `reconcile` — durable weak-want servicing
- `identity` — node signing-key handling
- `host` / `channel` — network thread and its bounded event bridge
