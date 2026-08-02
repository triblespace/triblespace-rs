# Capability Auth

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate ships a chain-of-trust capability system on top of iroh's
TLS-verified peer identities. Every connection on the
`/triblespace/pile-sync/5` ALPN must present a capability before any
other op is served. This chapter explains the team model, the CLI lifecycle,
and the blob-reachability scope gate the relay enforces.

For the design rationale (single team root vs multi-root web-of-trust,
sign-the-bytes convention, embedded parent sig optimisation), see the
companion design notes in
[`triblespace-core/src/repo/capability.rs`](https://github.com/triblespace/triblespace-rs/blob/main/triblespace-core/src/repo/capability.rs)'s
module-level docs.

## Model

A team has **one immutable root keypair**, generated once at team creation and
used to sign exactly one explicit `FounderAnchor`. The anchor binds the root to
the founder key and the team's maximum scope. It is tagged
`KIND_FOUNDER_ANCHOR`, has no `expires_at`, and is the only non-expiring link in
the model. After signing it the root keypair is archived; it never operates
online. Like a CA: bootstrapping authority, not runtime authority.

The founder anchor is **not a credential**. It is valid only as the final
parent/root terminator of a proof and is always rejected as an `OP_AUTH` leaf.
The founder signs a separate finite operational self-cap beneath it. Founder
renewals are fresh siblings under the same anchor, so rotation can extend the
operational expiry without growing the chain or bringing the root key online.

Every operational capability is finite and chains through the anchor. Any
holder of a live capability can sign a sub-capability for someone else, as long
as the sub-cap's scope is a subset of their own. Verification walks the chain
back to the explicit anchor and then the team-root signature.

Each capability is two blobs stored in the pile:

- A **cap blob** — a `TribleSet` carrying `cap_subject` (the pubkey this cap
  authorises), `cap_issuer` (the pubkey that signed it), and
  `cap_scope_root` (the entity id anchoring the scope facts inside the same
  blob). An operational cap carries exactly one `metadata::expires_at` and no
  cap-kind tag. A founder anchor instead carries exactly the
  `KIND_FOUNDER_ANCHOR` tag on that entity and no expiry. Mixed or unknown
  shapes fail closed.
- A **sig blob** — a `TribleSet` with `sig_signs` (handle of the cap
  blob) plus `repo::signed_by` + `signature_r` + `signature_s`,
  reusing the existing commit-signature attribute conventions.

Signatures attest directly to the cap blob's canonical bytes, not to its handle.
This differs from repository commit signatures, which attest to the commit's
content blob bytes; both remain hash-agnostic across future handle-scheme
changes.

For delegated caps, the **leaf sig blob** carries the recursive parent proof:
`sig_parent_cap` names the parent cap and `sig_embedded_parent_proof` points to
the parent signature entity in that same sig blob. Chain references never live
in a cap blob; a cap remains a pure `(subject, issuer, scope, lifetime-kind)`
declaration. At N capability levels, cold verification therefore needs one sig
blob plus N cap blobs instead of a separate sig blob for every parent.

Verification is deliberately stricter than checking a sequence of signatures:

- `MissingBlob` names the exact typed sig or cap handle required next; the
  verifier never scans arbitrary blob words for candidate links.
- `MAX_CHAIN_DEPTH = 32` counts the leaf as level one and rejects before a
  thirty-third parent fetch.
- Every delegation splice requires the parent cap's subject to equal the child
  cap's issuer.
- Every signature-proof entity has the intrinsic id of its standalone four
  facts: signed-cap handle, signer, and the two signature components. Embedded
  entities physically omit the signed-cap fact, but verification reconstructs
  it from the traversed edge and checks the id. Together with the one exact
  linear proof shape, this prevents entity renaming, unrelated entities, or
  extra proof attributes from minting content-distinct encodings of the same
  signed chain.
- Each child scope must be a subset of its parent's scope. A branch-restricted
  admin delegates only within that restriction; `PERM_ADMIN` is not a bypass.
- The root-signed link must be the exact founder-anchor shape. An anchor with an
  expiry, an ordinary finite root cap, a nonroot anchor, or an anchor offered as
  the leaf is rejected.
- The verified authority expires at the earliest **operational** expiry in the
  chain. The anchor contributes no artificial year-9999 deadline, and the
  effective deadline is rechecked during live use.

## Team Lifecycle (CLI)

The `trible team` subcommands cover the full lifecycle. Commands which mutate
or inspect durable policy name the pile explicitly; one-shot request and
delivery network calls do not require a separately running CLI process.

```
trible team create --pile PATH [--key KEY_PATH]
    Mint a new team root keypair, sign the one non-expiring founder
    anchor, then use the founder key to sign a separate finite
    operational self-cap. Persist all four blobs and the complete
    founder credential pin. Prints the team root pubkey (publish this
    to peers), root SECRET (archive offline), anchor sig handle
    (rotation/recovery authority), and finite cap-sig handle (the only
    one presented to OP_AUTH).

trible team invite --pile PATH --team-root HEX --cap HEX --key ISSUER
                   --invitee HEX --scope (read|write|admin)
                   [--legacy-pin HEX]...
    Issue a sub-capability to another peer. --cap must name the
    issuer's finite operational sig, never the founder anchor.
    ISSUER must hold a cap that subsumes the requested scope. The
    invitee's pubkey appears on its own (use
    `trible pile net identity` on the invitee's machine to print it).
    `--legacy-pin` restricts only the current blob
    RPC's mutable-pin roots; it cannot name a StrongPin branch. Prints the
    invitee's cap-sig handle.

trible team request-join --pile PATH --admin HEX
                         --scope (read|write|admin) [--key PATH]
    Send an OP_REQUEST_CAP to an admin's running daemon asking to
    be issued a capability. The exact partial request is stored in the
    requester's pile before network I/O, so a first delivery must match
    deliberate local intent. The admin sees the request on their
    pending-requests pin (`team list-pending`); after `team approve`
    the freshly-signed cap arrives via the auth-handshake ALPN.

trible team approve --pile PATH --entry HEX --team-root HEX
                    --cap HEX [--key PATH]
    Approve a pending request, sign the cap, dispatch it back to
    the requester, and add a renewal-policy entry so the local
    daemon keeps the cap renewed.

trible team retract --pile PATH --entry HEX
    Stop auto-renewing one (subject, scope) entry. The peer's
    chain dies at its next natural expiry. Pure local decision —
    no broadcast, no transitive cascade. This is the eviction
    primitive: there is no team-root-signed revocation blob in
    the descriptive-caps model.

trible team list --pile PATH
    Audit summary: per-cap detail line (issuer → subject, scope,
    expiry — sorted soonest-expiry-first).

trible team list-pending --pile PATH
    Incoming join requests awaiting approval.

trible team list-issued --pile PATH
    Renewal-policy entries this node is keeping renewed.

trible team show --pile PATH --cap HEX [--verify TEAM_ROOT_HEX]
    Walk one chain end-to-end. Prints each level with subject,
    issuer, scope, kind, expiry (absent on the founder anchor), cap blob
    handle, proof position (leaf blob or embedded parent), and
    a signer-matches-issuer (`✓` / `✗ MISMATCH`) check. Bounded
    by MAX_CHAIN_DEPTH=32; chains beyond root render the embedded
    parent sig as `(embedded in level above)`. Use when `list`
    shows a cap is present but a connection still fails — `show`
    surfaces structural mismatches (signer ≠ issuer, missing
    parent sig fields) that the summary view hides.

    `--verify <TEAM_ROOT_HEX>` (or env `TRIBLE_TEAM_ROOT`)
    additionally runs `verify_chain` against the given team root
    and reports `✓ VERIFIED` or `✗ FAILED — <VerifyError>` —
    the same code path the relay's `OP_AUTH` uses, so the
    result is the local-side rehearsal of what a real connection
    attempt would produce. Add `--expected-subject HEX` to
    override the default subject check (the leaf cap's declared
    `cap_subject`) for subject-substitution-attack detection.
```

A typical bootstrap flow:

```bash
# Founder, on machine A:
$ trible team create --pile team.pile --key founder.key
team root pubkey: 1a8a6a9d8ca1da67facab373de21233b...
team root SECRET: <archive offline>
founder anchor sig: a1c4a5f33b4d...
founder cap (sig):  4e6e02d51c3676ece1eea9094f8e9d76...

# Invitee, on machine B:
$ trible pile net identity --key invitee.key
node: e825b3a8d387b4dae1720b0edcbfaa9e...

# Founder, on machine A:
$ trible team invite --pile team.pile \
    --team-root 1a8a6a9d... \
    --cap       4e6e02d5... \
    --key       founder.key \
    --invitee   e825b3a8... \
    --scope     read
issued cap (sig): 7afe59e7f895b23f05452ff7919e12e4...
```

The invitee then runs the relay (or any pile-net peer) with
`TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP` set:

```bash
$ TRIBLE_TEAM_ROOT=1a8a6a9d... \
  TRIBLE_TEAM_CAP=7afe59e7... \
  trible pile net sync /path/to/their.pile --peers <founder-id>
```

Capability verification can complete a chain that the receiving peer does not
already hold without exposing the ordinary authenticated blob API. The typed
verifier reports the exact sig or cap handle it needs next. Auth-handshake v2's
proof-member operation asks the presenting peer for only that handle, resumes
verification, and repeats within the verifier's resource envelope. There is no
`OP_CHILDREN` operation or generic reference scan.

A proof locator is not a secret and must not be treated as a bearer token.
Before returning one member, the server verifies the named leaf chain entirely
from its own local snapshot for the named subject and confirms that the exact
requested handle was touched by that verification. Ordinary `OP_AUTH` still
binds the verified leaf subject to the caller's TLS identity.

Every proof member is limited to the handshake blob size; blob count, aggregate
bytes, total load time, and concurrent pre-authentication work are bounded
independently. Proof members fetched for a cold `OP_AUTH` remain in that
connection's verification map and are not appended to the pile. A capability
**delivery** is different: after full verification, its leaf pair and required
parent-proof bundle are admitted to the policy thread as one bounded event. The
policy thread stores the complete selected bundle before publishing the active
credential pin.

### Durable credential authority and recovery startup

The per-team `KIND_TEAM_CAP` pin is the authority for a peer's outbound
identity. It atomically names the current finite `(cap, sig)` pair and, for the
founder only, retains the founder-anchor sig as rotation authority. The anchor
is never returned as the current auth cap. A configured cap handle can seed an
empty pile only after full local verification and durable promotion; once the
pin exists, stale process configuration cannot override it.

Normal startup requires the pinned operational chain to be live. There is one
narrow recovery case: if the pinned chain is expired but its signatures,
founder anchor, proof shape, subject, scopes, and intervals all verify exactly,
the daemon may start **recovery-only**. It does not present the expired sig in
outbound `OP_AUTH` and cannot perform ordinary authorized work. A founder can
locally issue a fresh finite sibling from the retained anchor; an ordinary
member can accept the exact authorized renewal delivery. Once that fresh
credential is durably selected, ordinary operation resumes. Missing blobs,
malformed state, bad signatures, wrong roots, and wrong subjects still fail
startup loudly rather than being mislabeled as recovery.

Core keeps that distinction explicit: `verify_chain_allow_expired` verifies
the complete proof and computes its effective expiry for recovery
classification, while ordinary `verify_chain` additionally requires that
expiry to be live. Network authorization always uses the latter.

With no operational credential, the zero `self_cap` sentinel is server-only:
inbound serving and the open recovery/delivery channel can run, while outbound
authenticated operations predictably remain unavailable.

## Wire Protocol

Pile-sync v5 (`/triblespace/pile-sync/5`) is intentionally small:

| Op            | Byte | Meaning                               |
|---------------|------|---------------------------------------|
| `OP_GET_BLOB` | 0x02 | Fetch one in-scope blob by hash       |
| `OP_AUTH`     | 0x05 | Present a capability signature handle |

The retired bytes belonged to `OP_LIST`, `OP_CHILDREN`, and `OP_HEAD`.
Protocol v5 has no enumeration, transitive child walk, scalar branch state, or
remote write operation. It is an authenticated, read-only content protocol.

The **first stream** on every connection must be `OP_AUTH`. The server verifies
the referenced sig blob and walks back to the team root through
`sig_parent_cap` handles and the embedded proof entities carried by that sig
blob. If a required member is absent locally, verification names that exact
handle for the bounded proof-member loader before resuming. The server then
accepts (`AUTH_OK = 0x00`) or rejects (`AUTH_REJECTED = 0x01`). Subsequent
streams inherit the verified capability; there is no per-stream re-auth.

If the first stream is not `OP_AUTH`, or its capability is rejected, the
server closes the connection. A later `OP_AUTH` cannot replace the verified
connection identity: that stream receives `AUTH_REJECTED`, while the existing
authenticated connection state remains unchanged.

The host bounds unauthenticated work, authenticated connections, and executing
post-auth streams independently. It also permits at most one live inbound
pile-sync connection for a subject, so opening more connections cannot multiply
one identity's share of the global stream pool. Admitted stream tails retain
both the subject lease and authenticated-connection permit, even after their
accept loop exits. A connection-lifetime timer closes idle authority at its
authentication-time monotonic deadline, while each post-auth operation has its
own deadline and rechecks epoch expiry only after its complete request frame.
Expiry clears the shared auth state and closes the connection.

### Auth-handshake v2

The open onboarding channel uses a separate
`/triblespace/auth-handshake/2` ALPN. TLS supplies the peer identity, and every
stream is one-shot:

| Op                               | Byte | Meaning                                |
|----------------------------------|------|----------------------------------------|
| `OP_REQUEST_CAP`                 | 0x01 | Submit a partial capability request    |
| `OP_DELIVER_CAP`                 | 0x02 | Deliver a signed cap and proof bundle  |
| `OP_FETCH_CAPABILITY_BLOB`       | 0x03 | Fetch one exact verified proof member  |

Request, delivery, and delivery-confirmation event queues are separately
bounded. For `OP_REQUEST_CAP` and `OP_DELIVER_CAP`, `STATUS_OK` means only that
the complete payload obtained a queue slot; it is **not** an acknowledgement
that policy state or blobs are durable. A storage or policy failure after
admission can still decline the operation.

The durable pending-request map is capped at 1,024 requesters. One requester
gets one `Pending` payload until a local actor approves or rejects it: exact
replay is a no-op, and a different payload cannot churn the outstanding slot.
After local disposition, the same requester may open that stable slot again.

On the requesting side, `team request-join --pile PATH` records the exact
partial capability before sending it. A first delivered credential must match
that local issuer, subject, scope ceiling, and expiry ceiling. The expectation
is retained after an ambiguous network outcome and consumed only after the
selected credential becomes active. First activation is a recoverable journaled
transition: the exact pending-request head is claimed as `Activating` only after
the complete proof bundle is durable, then the team credential is installed and
flushed, and finally that exact activation head is cleared. A concurrent newer
request cannot be deleted or authorize a stale selection; startup can finish an
interrupted activation from its durable candidate handles.

Once an active credential exists, delivery selection is monotone: a candidate
must keep the same subject and issuer, must not weaken the current scope, and
must not shorten the verified chain's effective expiry. Delayed, reordered, or
attenuated deliveries therefore cannot roll active authority backwards. A
queued delivery's expiry is checked again at policy application time rather
than relying only on the earlier network-thread verification.

## Current Blob Scope Gate

Capabilities encode their scope as tribles hung off `cap_scope_root`:

- One or more `metadata::tag: PERM_*` triples granting permissions
  (`PERM_READ`, `PERM_WRITE`, `PERM_ADMIN`).
- Zero or more `scope_branch: <branch_id>` triples restricting the
  permission to a specific branch. An empty branch-restriction set
  means "all branches".

`scope_branch` currently carries a legacy 16-byte mutable-pin id. It cannot
name an exact StrongPin `(author key, name handle)` identity and must not be
used as authorization for signed-assertion ingest. The assertion-replication
milestone therefore includes a new exact-identity scope schema (or an explicit
intentionally broader policy).

For the current read-only RPC, a branch-restricted capability limits blob
access by reachability from matching mutable-pin roots in the server's local
serving snapshot. Guessing a hash outside that reachable set does not bypass
the restriction: `OP_GET_BLOB` returns the same missing sentinel as an absent
blob. The ALPN exposes no operation that enumerates the roots or their children.
If snapshot construction fails, the peer clears its prior serving view instead
of continuing with stale authorization data.

The same missing sentinel is returned when an in-scope blob exceeds the
256 MiB `GET_BLOB` transport envelope. The sender checks its shared store view
before making an owned response copy; the receiver independently checks the
declared length before allocating. This is a transport limit, not a local blob
validity rule.

Unrestricted caps (`granted_branches() == None` — no `scope_branch`
tribles) short-circuit to "every present blob is in scope".

Permission semantics mirror `scope_subsumes`: `PERM_WRITE` and `PERM_ADMIN`
imply `PERM_READ`; `PERM_ADMIN` is required to delegate sub-capabilities but
does not erase a resource restriction inherited from a parent.

`scope_branch` is the legacy mutable-pin authorization key used by the current
blob RPC only. The old mutable-HEAD gossip and tracking machinery has been
deleted, and this field must not be interpreted as a replicated StrongPin
assertion identity. Assertion replication needs an exact
`(author, name-handle)` scope schema and a separate foreign-author admission
policy.

## Eviction

There is no team-root-signed revocation blob. The descriptive-caps
model evicts peers via **per-issuer non-renewal**: every operational cap carries
a short natural expiry (default 30 days), the issuer's running
daemon refreshes the cap before that expiry as long as a
**renewal-policy entry** says it should, and `team retract` deletes
the entry. The peer's chain dies at the next natural expiry. The
decision is local to the issuer — nothing propagates, nothing
cascades, nothing has to be signed by the team root.

This trades the "instant network-wide revocation" property for
several real wins:

- **No revocation rescan on every snapshot refresh.** Previously
  `update_snapshot` walked every blob looking for `(rev, sig)` pairs
  signed by the team root; that was a CPU hotspot on quiescent peers.
  The refresh path is now a near-no-op snapshot swap.
- **No `HashSet<VerifyingKey>` shared state.** The old model needed
  a process-wide revocation set, written from the snapshot scanner
  and read from every chain verification. Removing it dropped a
  cross-thread synchronisation point.
- **No team-root keypair in normal operation.** Issuing a revocation
  required the team root SECRET to sign. Now the root SECRET lives
  in cold storage after signing the founder anchor; every day-to-day operation
  (founder rotation, invite, approve, retract) uses the founder key or a finite
  admin cap.
- **No distributed revocation ordering.** Transport no longer has to deliver a
  revocation blob *before* an affected cap is verified. Approved capabilities
  and renewals travel through the direct auth-handshake path; everything else
  is local issuer policy.

The trade-off: there's no way to immediately invalidate a
compromised key network-wide. The mitigation is to keep natural
expiries short (the 30-day default is a starting point, not a
hard rule) and to ensure issuers stop renewing the moment they
notice. For acutely sensitive teams the natural-expiry window can
be tightened to hours.

Renewal happens via the same `OP_DELIVER_CAP` path that `team
approve` uses: the issuer's daemon signs a fresh finite sibling with a
later effective expiry, dispatches it to the subject's daemon over the
auth-handshake ALPN, and the subject atomically replaces the operational pair
on its durable team-cap pin. A founder's sibling points directly to the retained
anchor rather than to its expiring predecessor, so repeated rotation stays at
constant proof depth. The old operational blobs become unreachable from the
pin; the founder anchor remains pinned as recovery/rotation authority.
The daemon reconciles the founder's unique non-retracted `(subject, scope)`
self-policy entry against the verified active credential before deciding what
is due. This repairs either side of a crash between credential activation and
policy bookkeeping without reopening a narrower self-policy entry or minting an
unnecessary second sibling. Scheduling records the verified chain-effective
deadline, never a leaf deadline that outlives one of its parents. For this
local entry, the cleared delivery marker is also the durable host-publication
journal: policy and proof blobs are flushed, a coherent serving snapshot and
outbound credential are published, and only then is delivery marked complete.
`team list-issued` shows the renewal-policy entries this node is
keeping renewed; `team retract --entry HEX` removes one.

## `PeerConfig` Surface

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_addr],
    team_root: team_root_pubkey,            // 32 bytes — the team's CA
    self_cap: bootstrap_cap_sig_handle,     // imported only if no durable pin
});
```

There is no gossip/direction switch: scalar HEAD replication has been removed,
while lazy weak-want reconciliation is controlled independently (for the CLI,
with `--no-lazy`). There is no `Default` impl: every peer construction site
must specify a team root because auth is mandatory. The CLI's single-user
team-of-one fallback sets `team_root = signing_key.verifying_key()`
and `self_cap = [0u8; 32]`; this is a server-only sentinel, not a synthesized
root-signed finite capability.

For a hosted relay running for a team, the operator only needs:

- 32 bytes: the team root pubkey
- a durable team-cap pin containing the relay's finite operational cap-sig
  pair (a configured handle may bootstrap that pin once after verification)

That's it. No per-user accounts, no shared secrets, no team
configuration database. Caps live in the pile alongside everything else;
approval and renewal deliver them directly over the auth-handshake ALPN.
