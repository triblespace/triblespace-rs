# Distributed Sync

`triblespace-net` synchronizes collections rather than exposing one ambient
store inventory. Its protocol follows the same decomposition as the collection
model:

1. stock `iroh-gossip` announces that an endpoint has a new opaque state for
   one collection;
2. one READ(C)-authorized PATCH walk repairs the exact evidence which can
   change that collection's value; and
3. exact blob handles fetch only the immutable bytes a resolver actually
   chooses through a collection-independent, mutually authenticated bearer
   protocol.

No global team, mutable roster, durable OFFER/GOSSIP bit, or second replicated
inventory is needed. The collection descriptor already states independent
READ and WRITE policy, and iroh authenticates the endpoint key on each direct
connection.

## Four independent capabilities

The boundaries are deliberately small:

```text
know C           -> join C's wake topic and learn (origin, opaque state root)
prove READ(C)    -> receive and repair C's authorization evidence
know H           -> derive its opaque locator, discover providers, and authorize H
satisfy WRITE(C) -> make a signed COMMIT active in C
```

`C` is the exact 32-byte collection descriptor handle. `H` is an exact blob
handle. Knowing either value is already unforgeable naming power, but they do
different jobs: `C` discovers a collection participant, while `H` is the bearer
capability and private discovery secret for one exact immutable value. The
provider directory sees only separately domain-separated KDF images and
endpoint-bound tokens, never either raw handle.

READ and WRITE are independent `AdmissionPolicy` values embedded in the
descriptor. Each is either `Open` or a canonical quorum over Ed25519 roots with
one semantic threshold. A derived collection chooses its own policies. Source
ancestry, routing knowledge, and possession of a blob do not silently supply
collection authority. Downstream delegation is a restriction signed into each
proof path, not a second descriptor threshold.

Local stores remain permissive grow-only ledgers. They may contain a COMMIT
whose signer does not currently satisfy WRITE(C), or a proof irrelevant to any
resident collection. Admission is applied when a snapshot is observed. Later
proof evidence may activate an old commit without rewriting or retracting it.

## The collection repair product

For one collection, semantic repair derives two independent grow-only sets:

- every structurally valid native collection record naming exact C: signed
  `COMMIT`s independent of current WRITE(C) admission, plus unsigned `MERGE`
  and `DERIVE` equations; and
- every self-contained native proof which is structurally relevant to exact
  READ(C) or WRITE(C) and begins at that action policy's roots.

Each set is represented by an immutable BLAKE3-Merkle PATCH. Collection
records are keyed physically by the full 32-byte fingerprint of their exact
canonical value; authorization evidence is keyed by its 32-byte proof ID and its
repair leaf is the complete canonical native proof body. There is no companion
claim blob or authorization closure. The opaque semantic repair root
commits to C, both PATCH roots, and both leaf counts under a versioned domain.

The authorization projection is structural rather than a snapshot of who is
admitted now. Expired, not-yet-valid, delegate-only, and quorum-incomplete
branches remain immutable evidence. Time, mode, and quorum are derived checks
at the operation instant. Each root path is evaluated independently; no
fixed-point over sibling paths can create delegation support.

The only initial handoff is the delegation itself: a grantor may give the new
subject the self-contained proof bytes. That is the capability invitation
boundary, not a Secrets-specific delivery channel. Once one collection
participant has the proof, authorization-evidence repair distributes that one
record to READ(C) peers without a second content-acquisition phase. A Secrets
writer can therefore derive a restricted collection's current finite READ
audience from the same snapshot and materialize recipient envelopes without a
separate envelope RPC or roster. Open READ remains explicitly non-enumerable.

This product matters. Synchronizing only collection records would miss the
case where a newly arrived proof activates an old COMMIT. Synchronizing a whole
proof store would disclose unrelated capability structure. The complete repair
algebra is therefore `Record × AuthorizationEvidence`, with both components
scoped to C. The receiver always derives its admitted view locally; record and
proof arrival therefore commute, and a publisher need not possess or present
its own WRITE grant merely to replicate an inert signed record.

Unsigned MERGE and DERIVE records remain computation evidence, but they are
first-class members of the exact-C record PATCH and ordinary collection
repair. Once present in a record store, an equation is reusable materialized
LSM work; warm readers do not execute its join or mapping again. A frozen
semantic view nevertheless ignores a repaired equation until all of its direct
blob references were resident in that same snapshot.

## Opaque wakes over stock gossip

The `iroh-gossip` topic ID is a domain-separated one-way image of the collection
handle. Anyone who knows C can derive and join that topic, while generic gossip
routers do not learn raw C. There is no authorization handshake merely to hear
that something changed. The application payload is fixed width (145 bytes):

```text
version:u8 || endpoint_origin:32 || repair_root:32 || nonce:16 || signature:64
```

The collection handle is not repeated in the envelope, but it is included in
the signature transcript. Replaying identical bytes on another collection
topic therefore fails verification. The origin is the same Ed25519 identity as
the iroh endpoint and tells receivers which peer can answer repair.

A wake contains no record, proof, blob handle, leaf count, component root, or
human-readable collection metadata. It is a latency hint, not durable evidence
and not authorization. The fresh nonce makes repeated change and neighbor
announcements distinct.
Stock gossip supplies bounded duplicate suppression and an efficient
dissemination tree; bounded leased wake-origin sampling supplies eventual
anti-entropy when a wake is delayed or missed.

Neighbor churn stays inside stock gossip: its active and passive views repair a
`NeighborDown`, and a reported `Lagged` event does not end the subscription.
The host treats lag as a reason to advance exact repair, not as a second mesh
algorithm. If the topic stream itself ends, configured endpoints plus a bounded
recent set of signed and DHT-discovered origins seed the replacement
subscription.

## READ(C)-authorized exact repair

After observing a changed wake root—or when periodically sampling live signed
wake origins—a node opens one bidirectional collection-repair stream to that
origin. Its hello names C and may carry a bounded set of self-contained READ
proofs for cold bootstrap. The server admits the TLS-authenticated client only
from READ(C) evidence in its pinned local projection before returning any
manifest. Unknown hello proofs are signature/root checked and stored inertly.
The current session remains rejected; a new coherent snapshot and session can
admit the now-resident proof without fetching any companion blob. For `Open`
READ the bootstrap is empty. A WRITE-only publisher needs no READ authority
merely to serve an authorized replica.

The server loads one immutable repair overlay for C and applies the
descriptor's exact READ policy at one instant. Rejection returns no manifest.
On admission it returns record and authorization-evidence PATCH summaries plus
the same opaque root. The client may then walk only differing prefixes and
receive missing leaf bodies:

- canonical signature-valid `COMMIT(C)` records, whether active or inert;
- native structurally relevant READ(C)/WRITE(C) proofs.

Each proof leaf already contains the complete path and all of its restrictions.
Records may land before sufficient WRITE proof evidence and remain harmlessly
inactive until a later snapshot derives admission.

The exact-repair scheduler samples at a 30-second cadence. Participant leases
last five minutes and every successful repair, including an identical result,
renews the selected peer. KDF(C) lookup is recovery-only: initial activation or
restart, exhaustion of every participant lease, or failure of every leased
candidate. Each collection permits one lookup in flight and retries an empty or
unsuccessful recovery with exponential backoff from one to 60 seconds. This
makes healthy steady-state DHT lookup load zero while preserving bounded
recovery after restarts and partitions.

Every request pins the manifest's expected component root. The server serves
the whole stream from one immutable overlay lease, so responses cannot splice
two moments together and need no historical-root cache. The client validates
node summaries, intrinsic leaf keys, record bodies, canonical proof bytes, and
proof signatures before insertion.

Exact-content GET is not part of this collection stream. An independent
exact-content request is authorized solely by knowledge of H.

Repair is one-way pull. Two peers converge by each eventually pulling after a
wake or periodic sweep. This keeps authorization and failure local to one
stream while set union makes direction irrelevant to the final value. A node
which holds no overlay for C answers unavailable rather than exposing a global
inventory.

## Blob transfer is lazy and bearer-addressed

Activation repair does not fetch descriptor dependencies, payloads, metadata,
attachments, or derived artifacts. It transfers the lattice evidence needed to
decide what exists. A resolver can then select the cheapest resident
support-equivalent cover and request only the missing immutable handles that
matter to that computation.

Knowledge of a full content hash H is the read capability for those exact
bytes and the secret needed to discover them. Publication and lookup derive a
full-width opaque locator under a dedicated domain:

```text
L = BLAKE3-KDF("triblespace.net/blob-locator/v1", H)
```

Every served resident blob may renew a soft lease at L on nearby XOR-DHT
nodes. The lease contains an independently domain-separated token bound to H
and the provider's authenticated endpoint ID. A requester who knows H rejects
forged candidate entries before dialing; the directory learns neither H nor
collection membership.

The direct stream also keeps H off the wire. The requester sends only L. The
provider resolves L in its resident locator index and proves knowledge of H
first, binding the proof to both authenticated endpoint IDs. Only after
verifying that proof does the requester send its role-separated proof of H.
The provider then returns bytes, which the requester hashes and compares with
H. A party which merely copied L therefore cannot learn H from a requester or
successfully serve bytes for it.

Fetching neither asserts collection membership nor activates a commit. It does
not consult C or READ(C), and creates a durable WANT only when the caller asks
the `WantStore` to record `Blob(H)`. Provider leases are bounded soft state and
may disappear without changing semantic data or local retention.

## Routing is process state

`Peer::new(store, key, config)` starts a production host immediately, as a
long-running repair daemon needs. `Peer::lazy(store, key, config)` keeps the same
store API but defers its host until the first absent-handle acquisition, explicit
network fetch, or collection activation. Resident reads, snapshots, local writes,
flush, and close start no thread or endpoint and build no serving/bearer index.
Its snapshots are still exactly the backing store's resident snapshot type.
Acquisition returns local observation errors rather than treating them as misses,
and a host startup failure leaves resident operations usable. Startup is attempted
once per peer; opening a new peer is an explicit retry. Activation logs startup
failures without activating the collection, while acquisition returns the error.

A foreground H-only reader need not activate any collection. It can use a distinct
ephemeral transport key, bootstrap endpoint routes, and a zero provider-publication
budget without borrowing its authorship signer's or a running daemon's endpoint
identity. Network acquisition requires an enabled Tokio runtime at the calling
async boundary; local-only operations need no runtime. `flush` makes local writes
durable, and explicit `close` withdraws host snapshots before closing the backend.
Neither operation starts networking or authors WANT.

Initial endpoint identities come from `PeerConfig` or the CLI. Configured relay
URLs only provide iroh transport paths; they are not collection participants or
KDF(C) rendezvous identities. A verified wake origin and DHT referrals may
become live routing candidates, but there is no synchronized PEER roster and no
durable peer record in the current protocol. Liveness, backoff, connection
pooling, DHT buckets, and provider leases are operational soft state;
restarting may forget them without losing semantic data and deliberately
re-enters KDF(C) discovery for each active collection.

One connection pool is shared by collection repair and bearer/DHT operations.
Iroh's transport authentication binds each connection to its endpoint ID.
There is no generic AUTH or SYNC_TEAM exchange: collection evidence is gated by
READ(C). Exact bytes are gated only by the endpoint-bound mutual proof of H.

## Lattice-aware sparse replication

The network does not force every replica to mirror every blob. Collection
records expose the same lattice known to local maintenance:

```text
COMMIT(C, a)       COMMIT(C, b)
       \             /
        MERGE(C, a, b, c)

DERIVE(D, c, d)
```

A node can repair the small semantic overlay and use its resident exact merge
and derivation results while planning a cover. Missing derived results are
computed by the ordinary live `ensure` path, which may acquire exact missing
dependencies and publishes missing `DERIVE` work only. `maintain` additionally
publishes deterministic size-tiered `MERGE` work. Those unsigned equations
repair as reusable computation evidence but grant no remote publication
authority; their referenced artifact blobs remain separate exact-H content.
Evidence and computation still converge by union; no central scheduler or
query planner is required.

Durable WANT remains orthogonal operational policy.
`WantRequest::Blob(H)` is the sole exact-content request. The reconciler
performs KDF(H) discovery and the mutual bearer proof directly from its coherent
store snapshot; no collection descriptor, repair overlay, or proof is involved. A
successful landing satisfies the request, while a DHT miss or failed proof
leaves it pending. `Merge(C,a,b)` and `Derive(D,input)` let one process state
demand while a network or worker process fulfills it. WANT grants no READ,
WRITE, retention, or membership semantics.

## Wire surface

Protocol version 23 keeps the direct operation set narrow:

| Operation | Code | Meaning |
|---|---:|---|
| `GET_BLOB` | `0x02` | locator-addressed, mutual-proof exact bearer transport |
| `PROVIDER_PUT` | `0x06` | renew this endpoint's opaque provider lease |
| `PROVIDER_GET` | `0x07` | obtain bounded candidates for one opaque key |
| `FIND_NODE` | `0x0C` | iterative XOR-DHT routing step |
| `COLLECTION_REPAIR` | `0x0D` | receiver-authorized record and authorization-evidence PATCH repair |

There is deliberately no store manifest, global inventory authorization,
push-broadcast record, receipt RPC, remote mutable head, or unpublish operation.

The CLI selects explicit collections and bootstrap peers:

```text
trible pile net sync DATA.pile \
    --collection COLLECTION_HANDLE [--collection COLLECTION_HANDLE ...] \
    [--peers ENDPOINT_TICKET ...] [--direction bidirectional|read-only|write-only]
```

Direction gates only the collection loop: `ReadOnly` pulls collection repair,
`WriteOnly` serves it, and `Bidirectional` does both. Every direction may
announce and serve resident exact blobs under bearer handle H, and may service
durable `Blob(H)` WANTs through KDF(H). These QoS choices do not participate in
collection identity or change which evidence is semantically valid.

## Convergence and failure model

- Concatenation, local insertion, and remote repair all perform set union.
- Duplicate records collapse by their complete canonical value; fixed-width
  indexes and the wire use a full-width BLAKE3 fingerprint of that value.
  Native capability proofs continue to collapse by their cryptographic
  identity.
- A missed wake only adds latency; periodic repair still converges connected
  readers.
- An invalid wake, record, proof, PATCH node, or blob fails that input and
  cannot retract previously accepted evidence.
- Missing blobs leave a semantic cover known but not yet materializable.
- A DHT miss says only that no live provider was found; it says nothing about
  whether H or its collection exists.
- Concurrent writers and offline replicas reconverge without preserving pile
  byte order.

The result is two orthogonal elemental loops: gossip plus READ(C)-gated PATCH
repair says *what changed in a collection*, while KDF(H) discovery plus mutual
bearer proof retrieves only the immutable bytes the local lattice resolver
decides to use.
