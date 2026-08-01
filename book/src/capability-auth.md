# Capability Auth

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate ships a chain-of-trust capability system on top of iroh's
TLS-verified peer identities. Every connection on the
`/triblespace/pile-sync/4` ALPN must present a capability before any
other op is served. This chapter explains the team model, the CLI lifecycle,
and the blob-reachability scope gate the relay enforces.

For the design rationale (single team root vs multi-root web-of-trust,
sign-the-bytes convention, embedded parent sig optimisation), see the
companion design notes in
[`triblespace-core/src/repo/capability.rs`](https://github.com/triblespace/triblespace-rs/blob/main/triblespace-core/src/repo/capability.rs)'s
module-level docs.

## Model

A team has **one immutable root keypair**, generated once at team
creation and used to sign exactly one capability — the founder's.
After that the root keypair is archived; it never operates online. Like
a CA: bootstrapping authority, not runtime authority.

All other capabilities **chain off the founder's** via delegation. Any
holder of a capability can sign a sub-capability for someone else, as
long as the sub-cap's scope is a subset of their own. Verification
walks the chain back to the team root.

Each capability is two blobs stored in the pile:

- A **cap blob** — a `TribleSet` carrying `cap_subject` (the pubkey
  this cap authorises), `cap_issuer` (the pubkey that signed it),
  `cap_scope_root` (the entity id anchoring the scope facts inside
  the same blob), and `metadata::expires_at`.
- A **sig blob** — a `TribleSet` with `sig_signs` (handle of the cap
  blob) plus `repo::signed_by` + `signature_r` + `signature_s`,
  reusing the existing commit-signature attribute conventions.

Signatures attest directly to the cap blob's canonical bytes, not to its handle.
This differs from repository commit signatures, which attest to the commit's
content blob bytes; both remain hash-agnostic across future handle-scheme
changes.

Non-root caps embed their parent's signature inline as a sub-entity
within the cap blob (`cap_embedded_parent_sig`). This halves cold-cache
verification fetch counts: at chain depth N, the verifier needs N+1
blobs instead of 2N+1.

## Team Lifecycle (CLI)

The `trible team` subcommands cover the full lifecycle. All four
operations work directly against a pile file — they don't require the
network thread.

```
trible team create --pile PATH [--key KEY_PATH]
    Mint a new team root keypair, sign the founder's capability with
    it, and write both into the pile. Prints the team root pubkey
    (publish this to peers), the team root SECRET (archive offline),
    and the founder's cap-sig handle (the founder's "credential" for
    OP_AUTH).

trible team invite --pile PATH --team-root HEX --cap HEX --key ISSUER
                   --invitee HEX --scope (read|write|admin)
                   [--legacy-pin HEX]...
    Issue a sub-capability to another peer. ISSUER must hold a cap
    that subsumes the requested scope. The invitee's pubkey appears
    on its own (use `trible pile net identity` on the invitee's
    machine to print it). `--legacy-pin` restricts only the current blob
    RPC's mutable-pin roots; it cannot name a StrongPin branch. Prints the
    invitee's cap-sig handle.

trible team request-join --admin HEX --scope (read|write|admin)
                         [--key PATH] [--pile PATH]
    Send an OP_REQUEST_CAP to an admin's running daemon asking to
    be issued a capability. The admin sees the request on their
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
    issuer, scope, expiry, sig blob handle, cap blob handle, and
    a signer-matches-issuer (`✓` / `✗ MISMATCH`) check. Bounded
    by MAX_DEPTH=32; chains beyond root render the embedded
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
founder cap (sig): 4e6e02d51c3676ece1eea9094f8e9d76...

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

The current protocol does **not** yet provide a bootstrap-safe transfer for a
capability chain that the verifier does not already hold. Ordinary authenticated
blob RPC cannot solve that circular dependency, and branch-restricted caps do
not make orphan cap/sig blobs reachable. Deployments must pre-seed the required
verification chains on serving peers for now; a narrowly scoped pre-auth chain
presentation protocol remains required before this flow is turnkey from two
cold piles.

Without those env vars the peer falls back to a single-user
team-of-one (`team_root = signing_key.verifying_key()`), which means
only their own caps will pass — useful for solo workflows but rejects
every other peer's cap.

## Wire Protocol

Protocol v4 (`/triblespace/pile-sync/4`) makes auth mandatory:

| Op            | Byte | Meaning                                  |
|---------------|------|------------------------------------------|
| `OP_GET_BLOB` | 0x02 | Fetch one in-scope blob by hash          |
| `OP_CHILDREN` | 0x03 | List in-scope child hashes of one parent |
| `OP_AUTH`     | 0x05 | Present a capability signature handle    |

Bytes `0x01` and `0x04` belonged to the retired `OP_LIST` and `OP_HEAD`
operations. Protocol v4 has no branch-list or branch-head RPC and no remote
write operation. It is an authenticated, read-only content protocol.

The **first stream** on every connection must be `OP_AUTH`. The server
fetches the referenced sig blob, walks back to the team root through
embedded parent sigs and `cap_parent` handles, and either accepts
(`AUTH_OK = 0x00`) or rejects (`AUTH_REJECTED = 0x01`). Subsequent
streams on the same connection inherit that verified capability for
the lifetime of the connection — there's no per-stream re-auth.

If the first stream is not `OP_AUTH`, or its capability is rejected, the
server closes the connection. A later `OP_AUTH` cannot replace the verified
connection identity: that stream receives `AUTH_REJECTED`, while the existing
authenticated connection state remains unchanged.

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

For the current read-only RPCs, a branch-restricted capability limits blob
access by reachability from the matching legacy mutable-pin roots in the
server's local snapshot. Guessing a hash outside those reachable sets does not
bypass the restriction: `OP_GET_BLOB` returns the missing sentinel and
`OP_CHILDREN` filters out-of-scope hashes. `OP_LIST` and `OP_HEAD` no longer
exist, so those root ids are not themselves enumerated through the ALPN
protocol.

Unrestricted caps (`granted_branches() == None` — no `scope_branch`
tribles) short-circuit to "every present blob is in scope".

Permission semantics mirror `scope_subsumes`: `PERM_WRITE` and
`PERM_ADMIN` imply `PERM_READ`; `PERM_ADMIN` is required to delegate
sub-capabilities. The reachability scan is recomputed per request
today; per-stream caching is a future optimisation for chain-walk-heavy
workloads.

Legacy HEAD gossip is a separate migration bridge. Its in-frame `publisher`
bytes are a routing hint, not a verified StrongPin author signature, and the
frame is not an assertion-admission decision. Receivers may retain the fetched
observation as a local tracking pin, but `pile net sync` never turns it into a
locally signed assertion automatically.

## Eviction

There is no team-root-signed revocation blob. The descriptive-caps
model evicts peers via **per-issuer non-renewal**: every cap carries
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
  in cold storage; every day-to-day operation (invite, approve,
  retract) uses a regular admin cap.
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
approve` uses: the issuer's daemon signs a fresh cap with a
later expiry, dispatches it to the subject's daemon over the
auth-handshake ALPN, and the subject pins it on the team-cap pin.
`team list-issued` shows the renewal-policy entries this node is
keeping renewed; `team retract --entry HEX` removes one.

## `PeerConfig` Surface

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig, SyncDirection};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_addr],
    gossip: true,                           // legacy HEAD observation mesh
    team_root: team_root_pubkey,            // 32 bytes — the team's CA AND
                                            // the gossip mesh id when gossip=true
    self_cap: my_own_cap_sig_handle,        // what we present on OP_AUTH
    direction: SyncDirection::Bidirectional,
});
```

There's no `Default` impl: every peer construction site must specify
a team root because auth is mandatory. The CLI's single-user
team-of-one fallback sets `team_root = signing_key.verifying_key()`
and `self_cap = [0u8; 32]` (which the remote rejects, signalling that
multi-user operation needs the env vars).

For a hosted relay running for a team, the operator only needs:

- 32 bytes: the team root pubkey
- 32 bytes: the relay's own cap-sig handle (the team grants it a
  read-or-better cap and the operator pastes that handle into the
  config)

That's it. No per-user accounts, no shared secrets, no team
configuration database. Caps live in the pile alongside everything else;
approval and renewal deliver them directly over the auth-handshake ALPN.
