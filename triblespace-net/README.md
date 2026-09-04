# triblespace-net

Collection-scoped anti-entropy for TribleSpace over
[iroh](https://www.iroh.computer). A peer retains an immutable semantic repair
overlay for each explicitly active collection. One repair stream reconciles
the exact product of two grow-only PATCHes: signature-valid exact-C COMMITs and
collection-scoped native READ(C)/WRITE(C) authorization proofs. It transfers
no blob bytes. Record inclusion is independent of WRITE admission; each
receiver derives its active view locally after records and proofs arrive in
either order.

The user-facing surface is `Peer<S>`, a synchronous store wrapper backed by an
async host. `Peer::refresh` drains verified repair events, crosses one storage
flush barrier, and only then replaces the immutable snapshots served to other
peers. There is no global team inventory, remote mutable head, replica roster,
or separate authority database.

## Getting started

Networking is a downstream capability and is consumed directly rather than
through the core TribleSpace facade:

```toml
[dependencies]
triblespace = "0.47"
triblespace-net = "0.47"
```

```rust,ignore
use triblespace_net::peer::{
    Peer, PeerConfig, ReconcileDirection, ReconcileQos,
};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let mut peer = Peer::new(
    pile,
    signing_key,
    PeerConfig {
        peers: vec![bootstrap_endpoint],
        qos: ReconcileQos {
            direction: ReconcileDirection::Bidirectional,
            ..ReconcileQos::default()
        },
    },
)?;
peer.activate_collection(collection_handle);

loop {
    peer.refresh();
    std::thread::sleep(std::time::Duration::from_millis(100));
}
```

Activation is ephemeral process state. It writes no OFFER/GOSSIP marker and
does not create an ambient collection registry.

## Authority and disclosure

The QUIC/TLS connection authenticates endpoint identities but grants no team
or collection authority. Every collection repair request names exactly one
collection. The repair client may present bounded native READ(C) proofs for
cold bootstrap. Same-session admission uses only self-contained
collection-scoped READ proofs already pinned in the server's local overlay. An
unknown proof is ingested inertly and may authorize a later retry, but it never
changes admission for the immutable current session and needs no companion
blob acquisition. The server verifies the TLS client before revealing a
manifest or PATCH leaf; the publisher itself needs no READ(C). Proofs are
non-secret authorization certificates. A caller without READ(C) receives no
collection manifest, PATCH leaf, record, authorization evidence, or root;
merely knowing C grants no disclosure.

DHT `FIND_NODE` and provider-directory operations use two independent opaque
namespaces. KDF(C) locates participants for READ(C)-authorized collection
repair. KDF(H) locates providers of exact resident content without naming a
collection. Each exact-content lease carries a token derived from H and the
provider endpoint, which a requester who knows H verifies before dialing.

The exact stream never sends H. The authenticated provider proves knowledge of
H first, bound to both TLS endpoint identities; only then does the requester
return its independently domain-separated proof. A false locator advertiser
therefore cannot make the requester disclose H or masquerade as a provider.
Returned bytes are accepted only when they hash to H. READ(C) is not consulted
by exact GET and remains exclusively the collection-repair disclosure boundary.

## Repair and wake

Periodic pairwise repair is authoritative anti-entropy. For each active
collection, the caller opens one bidirectional stream, establishes READ(C),
pins the returned record and authorization-evidence roots, and walks only
missing PATCH nodes. Authorization leaves carry canonical native proof bytes
only; each leaf is a complete signed path. Collection payload blobs never
travel in this stream.

Production iroh peers also subscribe to stock `iroh-gossip` topics keyed by a
domain-separated one-way image of the 32-byte collection handle. A 145-byte
nonce-v4 wake contains only version, signed origin endpoint, one opaque repair
root, and a fresh nonce. A mismatch schedules ordinary
READ-authorized repair from that signed origin; the wake itself carries no
authority or collection state. Missed or lagged wakes are harmless because
bounded sampled anti-entropy through leased signed wake origins remains active.

The host samples exact PATCH repair every 30 seconds. Signed wake origins and
KDF(C)-discovered endpoints receive five-minute participant leases, and every
successful repair—including an already-identical repair—renews the lease.
KDF(C) discovery is therefore a bootstrap and recovery path, not a heartbeat:
one lookup runs on initial activation or process restart, after every candidate
lease expires, or after every still-leased candidate has failed repair. Failed
discovery retries use one in-flight lookup per collection and exponential
backoff from one to 60 seconds. A healthy collection performs no periodic DHT
traversal.

Stock `iroh-gossip` owns neighbor-loss healing and reports lag without closing
the subscription. The host responds to lag by advancing ordinary exact repair,
not by replacing the mesh protocol. Configured endpoints and bounded recent
signed or DHT-discovered origins remain bootstrap candidates if a topic stream
does end and must be subscribed again. Configured iroh relays are transport
paths, not collection participants or rendezvous identities.

Direction is local policy:

- `Bidirectional` pulls active collections and serves admitted readers.
- `ReadOnly` pulls but does not serve local collection state.
- `WriteOnly` serves admitted readers but does not initiate collection repair.

This direction applies only to collection repair. Every mode may publish and
serve resident exact blobs under bearer handle H, and every mode may service a
durable `Blob(H)` WANT through the ordinary KDF(H) path.

Configured endpoint addresses bootstrap gossip and DHT routing only. Repair
targets come from signed wake origins or endpoint-bound KDF(C) leases.
Exact-content targets come from KDF(H) leases. Unrelated configured peers never
receive C or its proofs.

## Exact content

A durable `WantRequest::Blob(H)` asks the reconciler to discover and obtain
those exact bytes through KDF(H). It needs no collection descriptor,
activation, or READ proof. Collection repair has no payload-replication mode:
receiving a record is evidence convergence, not a request to traverse or copy
its referenced blob graph.

All exact requests share the one `Blob(H)` identity. A successful landing
satisfies the durable request locally; failed discovery leaves it pending.
Collection membership, proof state, and admission are irrelevant to that
exact-content operation.

The full model, wire formats, authorization boundaries, and CLI surface live
in the book's [Distributed Sync](https://docs.rs/triblespace/latest/triblespace/)
chapter.

## Crate layout

- `collection_activation` — per-collection record and authorization-evidence PATCHes
- `collection_session` / `collection_wire` — one READ-authorized repair stream
- `patch_repair` — root-pinned Merkle difference walker
- `peer` — synchronous store wrapper, durable admission, and local WANT intent
- `reconcile` — durable WANT observation and reproducible-operation fulfillment
- `provider` / `routing` — bounded bearer provider directory and XOR routing
- `protocol` — public direct-operation framing
- `host` — immutable overlays, connection pool, wake bridge, and scheduler
- `transport` — production iroh and deterministic simulation transports
- `identity` — persistent network signing-key handling
