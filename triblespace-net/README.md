# triblespace-net

Network transport for TribleSpace piles over
[iroh](https://www.iroh.computer). `Peer<S>` wraps one local store and provides:

- DHT content announcements and hash-verified blob transfer;
- capability-authenticated, scope-gated `GET_BLOB`;
- durable, peer-authored asserted wants for lazy fetching; and
- the capability request, delivery, renewal, and proof-bootstrap channel.

The network does **not** replicate generic asserted pins yet. Appending a
`PinAssertion` to one peer does not announce or admit that envelope or its
descriptor blob on another peer. The former scalar mutable-HEAD
gossip/tracking bridge and its `OP_CHILDREN` traversal RPC were deleted rather
than carried into the asserted-pin model.

## Capability model

The offline team root signs exactly one explicit, non-expiring
`FounderAnchor`: a tagged root-to-founder declaration with maximum scope and no
`expires_at`. The anchor is proof/rotation authority only and is rejected if
presented as an authentication leaf. Every credential used by `OP_AUTH` is a
separate finite operational capability. Founder renewals are siblings directly
under the retained anchor, so expiry can advance without an ever-growing proof
or another root signature.

Each node's durable outbound choice is an author-scoped recipient effect ledger,
not a scalar credential pin or process setting. `IntentDeclared`,
`IntentCanceled`, `CredentialAccepted`, and `FounderGrantSelected` are positive,
unionable facts. An ordinary first credential must descend from exact pending
team-scoped intent; later accepted credentials are monotone in issuer, scope,
and effective expiry. Founder authority additionally requires the matching
enabled policy grant. A usable selected founder grant has priority, while an
independently usable accepted credential is the fallback when founder selection
is inert.

Startup, refresh, delivery, and renewal derive authority from one coherent
assertion snapshot and reader. The host starts server-only, receives AUTH only
after a fresh Complete projection and coherent proof snapshot, and is reconciled
again whenever durable truth changes. There is no `PeerConfig.self_cap`,
activation journal, or configured bearer promotion. Expired credentials are
never selected for a new `OP_AUTH`; an operation which already captured its
predecessor may finish but cannot seed successor-pooled work. An exact expired
founder proof may remain only as the historical seed for a fresh direct-anchor
sibling.

Issuer-side lifecycle is one strong author-scoped asserted ledger.
`RequestObserved`/`RequestRejected` record exact request facts;
`GrantIssued` records exact credentials for stable
`(team root, subject, scope root)` grants; `CredentialAuthenticated` records
proof of one selected signature; and terminal `GrantDisabled` makes a grant
unusable and non-renewable without deleting its historical issuance or
revoking its already-issued chain. Every mutation and operational consumer
requires a fresh `PolicyLedgerResolution::Complete` for the exact signing
author. Missing or invalid closure fails closed.

`trible team list-issued --pile PATH [--author PUBKEY_HEX]` exposes that
complete grant view and prints the full canonical `GrantDisabled` selector for
each grant. `team retract --grant-event EVENT_HEX [--key PATH]` signs that
terminal fact with an existing author key; it does not delete policy history.

Each renewal tick resolves this peer author's recipient and issuer ledgers from
one coherent boundary; missing or invalid evidence defers the affected work. It
then considers exactly the selected founder `(team, subject, scope)` grant plus
enabled historical `Current` grants for remote subjects in the configured team.
Foreign teams and unrelated local-subject grants are inert after resolution.
Expired enabled currents remain seeds when their effective chain expiry is
inside the renewal window, although expiry prevents dispatch. Ordinary
successors are signed under the exact fresh local recipient authority and
asserted with `GrantIssued`; a single later
redispatch pass fresh-resolves policy, materializes the selected proof closure,
and sends only live, unauthenticated `usable_at(now)` winners.

Founder self-rotation is not self-delivery. It reconstructs the standalone
anchor from the selected historical proof, asserts a direct-anchor sibling,
then fresh-resolves both ledgers. Coherent serving-snapshot publication precedes
the outbound-host update. A durable selected winner that differs from the
process-local host observation is retried on startup or a later refresh without
another workflow marker. `team create` follows the same ordering law: publish
`GrantIssued`, publish `FounderGrantSelected`, then require a fresh Complete
joint view to select that exact usable credential.

First delivery is crash-recoverable without a workflow state machine. The
verified proof closure is durable before `CredentialAccepted`; host AUTH follows
only after a fresh resolution and coherent serving snapshot. A crash between
those boundaries leaves positive durable truth which startup or refresh simply
reconciles again. Cancellation or replacement racing acceptance remains visible
as inert contested evidence instead of deleting or overwriting either history.

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
});
```

When the pile has no live recipient-selected authority, the host remains
server-only. The internal all-zero sentinel is not a public setting or a
synthesized root-issued credential.

See the book's [Distributed Sync](../book/src/distributed-sync.md) and
[Capability Auth](../book/src/capability-auth.md) chapters for the exact current
boundary and team setup. The CLI surface is
`trible pile net {identity,status,sync}`. `sync` announces local content and
services only the local peer author's share of the wrapped store's canonical
global want-cache prefix. Foreign assertions can occupy a finite cache slot but
never become this peer's fetch work; over-budget assertions remain durable and
unfetched. `--no-lazy` disables the reconciler. It does not synthesize or
exchange branch state.

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
`STATUS_OK` for a request certifies that this receiver flushed the exact request
closure and durably appended its `RequestObserved` assertion. For delivery it
still acknowledges bounded queue admission rather than completed credential
activation. Stream tails retain their subject and global connection leases; an
independent monotonic expiry deadline closes idle authority, and each operation
authorizes only after its complete frame has arrived.

## Crate layout

- `peer` — synchronous storage wrapper, durable policy boundary, and lazy reads
- `protocol` — authenticated v5 blob wire format
- `handshake` — v2 request, delivery, and exact proof-bootstrap protocol
- `policy_ledger` — asserted incoming requests and issuer grant lifecycle
- `recipient_ledger` — local outbound intent, acceptance, and founder selection
- `reconcile` — author-scoped asserted-want servicing
- `identity` — node signing-key handling
- `host` / `channel` — network thread and its bounded event bridge
