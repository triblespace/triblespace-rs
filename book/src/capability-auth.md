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

### Asserted issuer policy

Each signing identity has one strong, author-scoped asserted policy ledger.
`RequestObserved` and `RequestRejected` describe exact requests;
`GrantIssued` names one exact signed credential for a stable
`(team root, subject, scope root)` grant; `CredentialAuthenticated` records
that the subject proved one exact issued signature; and terminal
`GrantDisabled` stops that grant from driving future work. These are positive,
unionable facts. Disabling a grant does not erase its credential or invalidate
an already-issued chain; it makes the grant unusable for redispatch and
renewal, so authority ends at the chain's natural expiry.

Policy is operational only through a fresh `PolicyLedgerResolution::Complete`
for one exact author. Missing closure or an invalid event fails closed, and no
command or daemon pass acts on a partial view. A complete grant view preserves
historical issuance separately from current usability: a coherent expired
credential on an enabled grant remains the historical `Current` renewal seed,
while `usable_at(now)` additionally requires that the grant be enabled and live.

## Team Lifecycle (CLI)

The `trible team` subcommands cover the full lifecycle. Commands which mutate
or inspect durable policy name the pile explicitly; one-shot request and
delivery network calls do not require a separately running CLI process.

```
trible team create --pile PATH [--key KEY_PATH]
    Mint a new team root keypair, sign the one non-expiring founder
    anchor, then use the founder key to sign a separate finite
    operational self-cap. Publish GrantIssued with the complete proof
    closure, then publish FounderGrantSelected in the founder's recipient
    ledger. Prints the team root pubkey (publish this to peers), root
    SECRET (archive offline), finite cap-sig handle, and both event
    handles. Renewal reconstructs the standalone anchor signature from
    the embedded selected proof rather than retaining a parallel handle.

trible team invite --pile PATH --team-root HEX --cap HEX --invitee HEX
                   [--key PATH] [--scope (read|write|admin)]
                   [--legacy-pin HEX]...
    Pre-authorize a sub-capability for another peer. --cap must name the
    issuer's finite operational sig, never the founder anchor.
    The issuer must hold a cap that subsumes the requested scope, and its
    signing key must already exist at the explicit or default path. Scope
    defaults to read. The command publishes GrantIssued;
    the running daemon later renews that asserted grant. This is issuer-side
    pre-approval, not a bearer credential or cold bootstrap: the printed
    signature handle is diagnostic, and first delivery still requires the
    invitee's independently recorded local request intent. The invitee's
    pubkey appears on its own (use
    `trible pile net identity` on the invitee's machine to print it).
    `--legacy-pin` restricts only the current blob
    RPC's mutable-pin roots; it cannot name an asserted branch pin. The
    invitee's cap-sig handle is printed for audit only.

trible team request-join --pile PATH --team-root HEX --admin HEX
                         [--scope (read|write|admin)] [--key PATH]
    Send an OP_REQUEST_CAP to an admin's running daemon asking to
    be issued a capability. An exact, team-scoped IntentDeclared event
    is published in the requester's recipient ledger before network I/O,
    so a first delivery must match deliberate local intent. The admin sees
    its durable
    `RequestObserved` assertion with `team list-pending`; after
    `team approve` the daemon redispatches the asserted credential via
    the auth-handshake ALPN.

trible team approve --pile PATH --request-event EVENT_HEX --team-root HEX
                    --cap HEX [--key PATH]
    Approve one exact canonical RequestObserved event (a full 32-byte
    handle) by signing the cap and asserting a provenance-bearing
    GrantIssued event. An existing issued-signature set is an
    idempotent success and never creates a sibling credential. The
    signing key must already exist; this command never generates it.

trible team reject --pile PATH --request-event EVENT_HEX [--key PATH]
    Assert RequestRejected for one exact request. Exact replay is a
    no-op. A late rejection of an issued-only request is refused;
    rejection alongside an existing issuance is reported as an
    independent fact and does not revoke the credential.

trible team retract --pile PATH --grant-event EVENT_HEX [--key PATH]
    Publish GrantDisabled for the exact grant selector printed by
    list-issued. The selector is the full 32-byte canonical
    GrantDisabled event handle, not a truncated id. The author's key
    must already exist and its ledger must resolve Complete. Exact
    replay is idempotent. The issued chain remains historical and dies
    at its natural expiry; there is no broadcast or transitive cascade.

trible team list --pile PATH
    Audit summary: per-cap detail line (issuer → subject, scope,
    expiry — sorted soonest-expiry-first).

trible team list-pending --pile PATH [--author PUBKEY_HEX]
    Show observed, rejected, pending, and every issued-signature fact
    for each exact request. Without --author, one policy author is
    detected from valid assertions without reading or creating a key;
    multiple candidates must be selected explicitly. Only a Complete
    view is displayed.

trible team list-issued --pile PATH [--author PUBKEY_HEX]
    Show every exact grant in one author's Complete policy view,
    including its full GrantDisabled selector, team, subject, scope,
    disabled state, historical issuance, selected cap/sig and effective
    expiry, authentication, and current usability. Author selection is
    identical to list-pending.

trible team show --pile PATH --cap HEX [--verify TEAM_ROOT_HEX]
                 [--expected-subject HEX]
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

`team create` follows the same ordering law as every other authority-changing
operation: licensing policy precedes recipient selection, and a fresh coherent
read witnesses both before the command exposes success. The `GrantIssued`
closure retains the operational pair and founder anchor without a parallel
credential pin. A crash after issuance but before founder selection leaves
valid policy material, but no selected operational authority.

A typical bootstrap flow:

```bash
# Founder, on machine A:
$ trible team create --pile team.pile --key founder.key
team root pubkey: 1a8a6a9d8ca1da67facab373de21233b...
team root SECRET: <archive offline>
founder cap (sig):  4e6e02d51c3676ece1eea9094f8e9d76...

# Founder identity, on machine A:
$ trible pile net identity --key founder.key
node: 72e48118d18a22b16b5f8a83eaf5bd3a...

# Keep the founder daemon running while requests and approvals arrive:
$ TRIBLE_TEAM_ROOT=1a8a6a9d... \
  trible pile net sync team.pile --key founder.key

# Invitee, on machine B, independently asks for the authority they want:
$ trible team request-join --pile invitee.pile \
    --team-root 1a8a6a9d... \
    --admin 72e48118... --scope read --key invitee.key

# Founder, on machine A, selects the exact observed request and approves it:
$ trible team list-pending --pile team.pile --author 72e48118...
request event: c47c00be...
$ trible team approve --pile team.pile \
    --request-event c47c00be... \
    --team-root 1a8a6a9d... \
    --cap 4e6e02d5... \
    --key founder.key

# Invitee runs server-only until the accepted delivery arrives.
$ TRIBLE_TEAM_ROOT=1a8a6a9d... \
  trible pile net sync invitee.pile --peers 72e48118... --key invitee.key
```

The request command records the invitee's exact partial capability before any
network I/O. Delivery is therefore selected against intent that originated on
the invitee's side, not against values copied from an issuer's offer. The
issuer retries an approved, unauthenticated grant; once the complete proof
closure arrives, the invitee publishes `CredentialAccepted` against the exact
intent basis. A fresh Complete recipient projection can then make it the
running host's outbound identity. `team invite` may record an issuer's
pre-approval for a known subject, but its printed handle never replaces this
recipient-local selection boundary.

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
policy thread stores the complete selected bundle before publishing its
`CredentialAccepted` effect.

### Durable recipient authority and recovery startup

There is no mutable "current credential" pin. Each identity instead has one
author-scoped recipient effect ledger. `IntentDeclared`, `IntentCanceled`,
`CredentialAccepted`, and `FounderGrantSelected` are signed, positive,
unionable facts; active authority is a projection of their complete causal
history rather than a scalar value overwritten by the last process to write.

An ordinary first credential is selectable only when its complete verified
proof descends from exactly one pending intent for the same team. Acceptance
names that intent as its basis. Cancellation, replacement, and acceptance
races remain visible in the merged history and become inert contested evidence
instead of silently recreating last-writer-wins selection. Later accepted
credentials remain monotone: they cannot weaken scope or shorten effective
expiry.

Founder authority is the conjunction of two ledgers: an enabled policy grant
for the local founder and a matching `FounderGrantSelected` recipient effect.
The founder selector names `(team root, scope root)`; the policy projection
supplies the deterministic exact credential. A usable selected founder grant
has priority over ordinary accepted authority. If the founder selection is
inert because its grant is unavailable, disabled, conflicted, or unusable, a
separately valid accepted credential may operate instead.

Startup, refresh, delivery, and renewal all resolve recipient and policy facts
from one assertion snapshot and one blob reader. This matters even though both
ledgers are monotone: two sequential snapshots could combine projections from
different moments into a state which never existed. An incomplete or invalid
recipient ledger fails closed. Incomplete founder policy makes that founder
selection inert; it cannot block an independently Complete, usable ordinary
acceptance. A publication receipt proves only that one event was appended;
every host effect follows a fresh resolution and a coherent serving snapshot.

Crash recovery is level-triggered. The durable projection is truth, while the
host's currently presented signature is merely process-local observation. If
they differ after startup, refresh, or an interrupted delivery, reconciliation
publishes the freshly derived live signature—or the all-zero server-only
sentinel—to the host. There is no activation journal to replay and no configured
bearer handle to promote.

Expired authority is never selected for a new `OP_AUTH` operation. Reconciliation
publishes the successor synchronously after its proof snapshot; credential-keyed
connection pooling prevents a predecessor fetch which completed discovery late
from contaminating successor work. As with any network revocation boundary, an
operation which already captured the predecessor may finish rather than being
retroactively canceled. An enabled expired founder grant may still be the
historical seed for a direct-anchor sibling renewal:
`verify_chain_allow_expired` validates and reconstructs its standalone founder
anchor, while ordinary `verify_chain` continues to require live authority for
network use. An ordinary member remains server-only until a live delivered
credential is accepted.

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
bounded. For `OP_REQUEST_CAP`, `STATUS_OK` is sent only after the exact request
closure is flushed and its `RequestObserved` assertion is durably appended.
`STATUS_REJECTED`
means the request definitely did not enter durable policy, either because of a
stable policy refusal or a failure before policy-loop admission. A persistence
error after admission instead yields `STATUS_INDETERMINATE`: append APIs cannot
promise that an error means no effect, so the requester retains its exact local
intent and may replay it idempotently. A timeout is ambiguous for the same
reason.
For `OP_DELIVER_CAP`, `STATUS_OK` still means that the fully verified payload
obtained a queue slot, not that a recipient acceptance event is durable or
currently selected.

For serialized writes against one receiver, the prospective policy view admits
at most 1,024 pending request identities and one pending identity per requester.
Those are local admission/resource guards, not replicated invariants. Copies may
be mutated independently and later unioned; every valid `RequestObserved` fact
survives, so the merged view may contain multiple pending identities for one
requester or exceed 1,024. Exact replay remains one fact, and local disposition
allows that receiver to admit another request.

On the requesting side, `team request-join --pile PATH --team-root HEX`
publishes the exact partial capability as `IntentDeclared` before sending it.
A first delivered credential must match that local issuer, subject, scope
ceiling, expiry ceiling, and team. The intent is retained after an ambiguous
network outcome; an explicit remote rejection publishes `IntentCanceled`
against the exact event rather than deleting state.

The policy thread stores and flushes the signature, cap, and complete proof
closure before it publishes `CredentialAccepted`. It then resolves the full
recipient history again, builds one coherent serving snapshot, and only then
updates outbound `OP_AUTH`. A crash at any boundary is repaired by the same
fresh resolution on startup or refresh. Delayed, reordered, attenuated, or
causally contested deliveries cannot roll active authority backwards, and a
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
name an exact asserted pin `(author key, descriptor handle)` and must not be
used as authorization for generic assertion ingest. An assertion-replication
protocol therefore needs a new exact-identity scope schema (or an explicit,
intentionally broader policy). A branch-specific UI may recover its name by
loading the canonical outer `StrongPinDescriptor` and then its wrapped
`BranchPinDescriptor`; the name itself is not the generic identity.

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
deleted, and this field must not be interpreted as a replicated asserted-pin
identity. Assertion replication needs an exact `(author, descriptor-handle)`
scope schema plus separate foreign-author and pin-kind admission policies.

## Eviction

There is no team-root-signed revocation blob. The descriptive-caps
model evicts peers via **per-issuer non-renewal**: every operational cap carries
a finite natural expiry, the issuer's running daemon refreshes it while the
author's asserted grant remains enabled, and
`team retract --grant-event ...` publishes the terminal `GrantDisabled` fact.
The peer's already-issued chain dies at its next natural expiry. The decision
is local to the issuer — nothing propagates, nothing cascades, and nothing has
to be signed by the team root.

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
compromised key network-wide. The mitigation is to keep natural expiries short
and to ensure issuers stop renewing the moment they notice. CLI-created initial
credentials and requests use a 30-day ceiling (further bounded by parent
authority), but that is not the daemon's renewal lifetime: each successor
targets `now + 2 * renewal_window`, again bounded by its live parent. The stock
`pile net sync` passes a one-hour window, so its successors target roughly two
hours. Library callers can choose a different window; the current CLI exposes
no lifetime knob.

Renewal happens via the same `OP_DELIVER_CAP` path that `team approve` uses for
ordinary subjects. Each tick resolves the local signing author's policy and
recipient ledgers from one coherent assertion boundary; missing or invalid
evidence anywhere defers the pass before team or subject filtering. The
ordinary work set is exactly the configured team's remote-subject grants with
an enabled historical `Current` whose effective chain expiry is inside the
renewal window. A current founder selection adds only its exact
`(team, local subject, selected scope)` self grant. Foreign-team and unrelated
local-subject grants are inert after resolution. An already-expired enabled
current remains a seed: expiry blocks dispatch but does not erase the history
needed to recover. `Conflicted` and disabled grants produce no renewal work.

For an ordinary grant, the issuer fresh-resolves its exact live local recipient
authority as parent, signs a later finite successor with the same scope facts,
and publishes a new `GrantIssued`. Publication is not selection: the daemon
then takes another fresh Complete resolution, materializes every selected proof
blob into the serving view, and sends only the enabled, live, unauthenticated
deterministic winner. The subject's daemon accepts the successor through the
normal recipient-ledger delivery path.

Founder self-rotation is deliberately local and has two distinct steps. The
founder reconstructs the standalone anchor from the selected historical proof,
signs and asserts a direct-anchor sibling, then fresh-resolves both ledgers. The
usable deterministic policy winner becomes operational only because the
recipient's founder selection still licenses that scope. The daemon publishes a
coherent serving snapshot and updates outbound `OP_AUTH`; it never delivers to
itself. A flush, snapshot, or publication failure leaves durable truth different
from the process-local host observation, so a later fresh tick retries without a
workflow marker. Repeated founder rotation therefore stays at constant proof
depth.

`team list-issued [--author PUBKEY_HEX]` shows the complete asserted grant view
and prints each full `GrantDisabled` selector. `team retract --grant-event
EVENT_HEX` signs that exact terminal fact; it does not delete history.

## `PeerConfig` Surface

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_addr],
    team_root: team_root_pubkey,            // 32 bytes — the team's CA
});
```

There is no gossip/direction switch: scalar HEAD replication has been removed,
while lazy author-scoped asserted-want reconciliation is controlled
independently (for the CLI, with `--no-lazy`). There is no `Default` impl:
every peer construction site
must specify a team root because auth is mandatory. The CLI's single-user
team-of-one fallback sets `team_root = signing_key.verifying_key()`. Without a
live authority in the recipient projection the host uses an internal all-zero
server-only sentinel; it is not public configuration and cannot synthesize a
root-signed finite capability.

For a hosted relay running for a team, the operator only needs:

- 32 bytes: the team root pubkey
- a local pile carrying the relay's recipient and policy assertion closure

That's it. No per-user accounts, no shared secrets, no team
configuration database and no bearer-handle environment knob. Caps and the
positive effects selecting them live in the pile alongside everything else;
approval and renewal deliver them directly over the auth-handshake ALPN.
