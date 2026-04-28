# Capability Auth

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate ships a chain-of-trust capability system on top of iroh's
TLS-verified peer identities. Every connection on the
`/triblespace/pile-sync/4` ALPN must present a capability before any
other op is served. This chapter explains the team model, the CLI
lifecycle, and the two-tier scope gate the relay enforces.

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

Signatures attest to the cap blob's canonical bytes (matching how
`Workspace::commit` signs commit metadata), not to a hash of those
bytes — keeping signatures hash-agnostic across any future change to
the handle scheme.

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
                   [--branch HEX]...
    Issue a sub-capability to another peer. ISSUER must hold a cap
    that subsumes the requested scope. The invitee's pubkey appears
    on its own (use `trible pile net identity` on the invitee's
    machine to print it). Prints the invitee's cap-sig handle.

trible team revoke --pile PATH --team-root-secret HEX --target HEX
    Issue a revocation blob, signed by the team root, against the
    target pubkey. Cascades transitively: revoking K invalidates
    every cap K signed and (transitively) every cap derived from
    those.

trible team list --pile PATH
    Audit summary: per-cap detail line (issuer → subject, scope,
    expiry — sorted soonest-expiry-first) plus the (revoker,
    target) pair for each verifiable revocation.

trible team show --pile PATH --cap HEX
    Walk one chain end-to-end. Prints each level with subject,
    issuer, scope, expiry, sig blob handle, cap blob handle, and
    a signer-matches-issuer (`✓` / `✗ MISMATCH`) check. Bounded
    by MAX_DEPTH=32; chains beyond root render the embedded
    parent sig as `(embedded in level above)`. Use when `list`
    shows a cap is present but a connection still fails — `show`
    surfaces structural mismatches (signer ≠ issuer, missing
    parent sig fields) that the summary view hides.
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
  trible pile net sync /path/to/their.pile --peers <founder-id> --topic team-graph
```

Without those env vars the peer falls back to a single-user
team-of-one (`team_root = signing_key.verifying_key()`), which means
only their own caps will pass — useful for solo workflows but rejects
every other peer's cap.

## Wire Protocol

Protocol v4 (`/triblespace/pile-sync/4`) makes auth mandatory:

| Op            | Byte | Meaning                                 |
|---------------|------|-----------------------------------------|
| `OP_LIST`     | 0x01 | List all branches and heads             |
| `OP_GET_BLOB` | 0x02 | Fetch one blob by hash                  |
| `OP_CHILDREN` | 0x03 | List blob hashes referenced by a parent |
| `OP_HEAD`     | 0x04 | Head hash of one branch                 |
| `OP_AUTH`     | 0x05 | Present a capability sig handle         |

The **first stream** on every connection must be `OP_AUTH`. The server
fetches the referenced sig blob, walks back to the team root through
embedded parent sigs and `cap_parent` handles, and either accepts
(`AUTH_OK = 0x00`) or rejects (`AUTH_REJECTED = 0x01`). Subsequent
streams on the same connection inherit that verified capability for
the lifetime of the connection — there's no per-stream re-auth.

Streams sent before OP_AUTH or after AUTH_REJECTED are silently
closed. The server doesn't leak a "you sent the wrong thing" error
back to the client.

## Two-Tier Scope Gate

Capabilities encode their scope as tribles hung off `cap_scope_root`:

- One or more `metadata::tag: PERM_*` triples granting permissions
  (`PERM_READ`, `PERM_WRITE`, `PERM_ADMIN`).
- Zero or more `scope_branch: <branch_id>` triples restricting the
  permission to a specific branch. An empty branch-restriction set
  means "all branches".

The relay enforces scope at two levels:

### Branch level (`OP_LIST`, `OP_HEAD`)

`VerifiedCapability::grants_read_on(branch)` filters which branches
the peer can see. Out-of-scope branches are silently dropped from
`OP_LIST` responses; `OP_HEAD` for an out-of-scope branch returns
`NIL_HASH` (indistinguishable from "branch doesn't exist", as far as
the wire is concerned).

### Blob level (`OP_GET_BLOB`, `OP_CHILDREN`)

A peer with branch-X-only scope could otherwise circumvent the branch
gate by guessing or probing raw blob hashes from branch Y. The
blob-level gate closes that hole: a hash is in scope only if it's
reachable (via 32-byte child chunks) from at least one branch head the
cap grants read on. Out-of-scope blobs surface as `None` (length =
`u64::MAX`) on `OP_GET_BLOB`; `OP_CHILDREN` filters its returned list
to in-scope hashes only.

Unrestricted caps (`granted_branches() == None` — no `scope_branch`
tribles) short-circuit to "every present blob is in scope".

Permission semantics mirror `scope_subsumes`: `PERM_WRITE` and
`PERM_ADMIN` imply `PERM_READ`; `PERM_ADMIN` is required to delegate
sub-capabilities. The reachability scan is recomputed per request
today; per-stream caching is a future optimisation for
chain-walk-heavy workloads.

## Revocation

Revocations are their own blob type — a small `TribleSet` carrying
`rev_target` (the pubkey being revoked) and `metadata::created_at`,
plus a sig blob of the same shape as cap signatures.

The relay maintains a `HashSet<VerifyingKey>` of revoked pubkeys.
Every chain verification step checks `revoked.contains(issuer)` and
`revoked.contains(subject)` — revoking key K invalidates every cap K
signed and (transitively) every cap derived from those, with no
restart needed.

Two ways revocations land in that set:

1. **Boot seed.** `PeerConfig.revoked` is loaded once at relay
   startup. Useful for hardcoded "always-revoked" lists.
2. **Live propagation.** Every `Peer::refresh` (which is auto-called
   on every read or write through the Peer) updates the served
   snapshot. The update path *also* rescans the new snapshot for
   `(rev, sig)` blob pairs signed by the configured team root and
   unions them into the live revoked set. A revocation blob gossiped
   into the pile is therefore picked up on the next snapshot refresh.

The set is monotonically growing — boot-time revocations remain in
even if the corresponding blob is later GC'd from the pile. Only
revocations signed by the configured team root are accepted; bystander
revocations are ignored.

## `PeerConfig` Surface

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};
use std::collections::HashSet;

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_id],
    gossip_topic: Some("my-team-graph".into()),
    team_root: team_root_pubkey,            // 32 bytes, the team's CA
    revoked: HashSet::new(),                // boot-time seed (usually empty)
    self_cap: my_own_cap_sig_handle,        // what we present on OP_AUTH
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
configuration database. Caps live in the pile alongside everything
else and gossip propagates them naturally.
