# Trible CLI

Trible CLI is a friendly companion for exploring and managing
[Tribles](https://github.com/triblespace/tribles-rust) and TribleSpace piles
from the command line.

This crate tracks `triblespace` releases (major/minor), and may ship independent patch releases.

## Installation

```bash
cargo install trible
```

Or, for local development:

```bash
cargo install --path .
```

## Quick Start

1. Create a new pile to hold your data:

   ```bash
   trible pile create demo.pile
   ```

2. Add a file as a blob. This command prints a handle for the stored blob:

   ```bash
   echo "hello" > greeting.txt
   trible pile blob put demo.pile greeting.txt
   ```

3. List the blobs in the pile to confirm the handle:

   ```bash
   trible pile blob list demo.pile
   ```

4. Retrieve the blob using its handle:

   ```bash
   trible pile blob get demo.pile <HANDLE> copy.txt
   ```

The file `copy.txt` now contains the original contents of `greeting.txt`.

## Usage

Run `trible <COMMAND>` to invoke a subcommand.

### Generate identifiers

- `genid` — generate a random identifier.

### Generate shell completions

- `completion <SHELL>` — output a completion script for `bash`, `zsh`, or `fish`.

### Work with piles

- `pile create <PATH>` — initialize a new empty pile without replacing an existing file; the parent directory must already exist.
- `pile diagnose check <PILE>` — verify pile integrity.
- `pile diagnose locate-hash <PILE> <HANDLE>` — scan raw pile bytes and report where a handle appears (blob header vs payload references).
#### Branches

- `pile branch list <PILE> [--signing-key <KEY> | --author <AUTHOR> | --all]` — list exact identities and their local `complete`, `partial`, or `tip-pending` resolution.
- `pile branch show <PILE> <BRANCH>` — inspect one exact descriptor, its assertions, frontier, missing ancestry, and derived read head.
- `pile branch log <PILE> <BRANCH>` — walk locally available ancestry from the resolver's candidate tips.
- `pile branch forget <SOURCE> <DESTINATION> <BRANCH>` — create a new local pile generation without that exact identity's assertion records. This is physical forgetting, not replicated deletion; the source remains untouched and synchronization can reintroduce the assertions.

`<BRANCH>` is always the full descriptor
`ed25519:<64 hex>/blake3:<64 hex>`. Its full-width generic pin digest is
diagnostic output and is never accepted as a selector. Empty branches, mutable
scalar heads, rename, consolidation, raw CLI authoring, and replicated deletion
are not part of the asserted-pin branch model. Publication goes through a
`Repository` workspace, which carries the authenticated branch-rank provenance
needed to sign a safe assertion.

Signing key format
- `branch list` can select the configured local author through `--signing-key` or the `TRIBLES_SIGNING_KEY` path. The file contains one 64-character hex Ed25519 seed. Commands never invent an ephemeral branch author.

#### Blobs

- `pile blob list [--metadata] <PILE>` — list stored blob handles. Pass `--metadata` to include timestamps and sizes.
- `pile blob put <PILE> <FILE>` — store a file as a blob and print its handle.
- `pile blob get <PILE> <HANDLE> <OUTPUT>` — extract a blob by handle.
- `pile blob inspect <PILE> <HANDLE>` — display metadata for a stored blob.

### Distributed pile sync

Built on `triblespace-net` (iroh QUIC + DHT). Direct blob RPC connections
authenticate through capability chains rooted at a team's pubkey. Branch
assertions are deliberately not transported yet: synchronization announces
content and services durable lazy-fetch wants without synthesizing branch
authority. `identity` and `status` are local diagnostic commands. See the
*Capability auth* section below for team setup.

- `pile net identity [--key PATH]` — print this node's iroh identity (auto-generates a key if missing).
- `pile net status [--key PATH]` — print the node id and trusted team root, including whether the root came from `TRIBLE_TEAM_ROOT` or the team-of-one fallback. Operational credentials are derived from the pile's recipient ledger, not bearer-handle configuration.
- `pile net sync <PILE> [--peers ID,...] [--key PATH]` — announce local blobs and service this peer author's durable signed wants over the team network. Reads `TRIBLE_TEAM_ROOT`; the root falls back to the node's own pubkey for a team-of-one, while a missing live recipient-selected credential leaves the node server-only. `--no-lazy` disables want reconciliation. Generic assertion replication still requires a dedicated protocol and admission policy.

### Capability auth

Chain-of-trust capability system for distributed pile sync. A team has one
immutable root keypair, used once at creation and then archived, which signs a
non-expiring founder anchor. The founder's finite operational credential and
future rotations are siblings beneath that anchor; ordinary capabilities
continue by delegation. See
[`book/src/capability-auth.md`](../book/src/capability-auth.md) for
the full design.

- `team create --pile PATH [--key KEY_PATH]` — mint a team root, sign one non-expiring founder anchor, issue a finite founder operational cap beneath it, publish `GrantIssued`, then publish `FounderGrantSelected`. A fresh coherent policy/recipient read must select that exact live credential before the root secret is returned. Prints the public team root, root secret to archive offline, operational handles, expiry, and both event handles.
- `team invite --pile PATH --team-root HEX --cap HEX --invitee HEX [--key PATH] [--scope (read|write|admin)] [--legacy-pin HEX]...` — pre-authorize a sub-capability and publish `GrantIssued`; the issuer key must already exist at the explicit or default path, and scope defaults to read. This is issuer-side policy, not a bearer credential or cold bootstrap: the printed signature handle is diagnostic, and first delivery still requires independently recorded request intent in the invitee's pile. The running daemon later renews the asserted grant. The issuer must hold a cap that subsumes the requested scope. `--legacy-pin` (repeatable) restricts the current blob RPC to reachability from mutable local-pin roots; it does not select an exact asserted branch pin.
- `team request-join --pile PATH --team-root HEX --admin HEX [--scope (read|write|admin)] [--key PATH]` — publish the exact team-scoped local `IntentDeclared`, then send `OP_REQUEST_CAP` to an admin's running auth-handshake daemon; scope defaults to read.
- `team approve --pile PATH --request-event EVENT_HEX --team-root HEX --cap HEX [--key PATH]` — approve one full canonical `RequestObserved` handle with an asserted, provenance-bearing `GrantIssued`; an existing issued-signature set is an idempotent success and the key must already exist.
- `team reject --pile PATH --request-event EVENT_HEX [--key PATH]` — assert rejection of one exact request without implying revocation of any independently issued credential; the key must already exist.
- `team retract --pile PATH --grant-event EVENT_HEX [--key PATH]` — publish terminal `GrantDisabled` for the full canonical selector printed by `list-issued`. The author key must already exist and the issuer ledger must resolve Complete. Exact replay is idempotent; the issued chain remains historical and dies at its natural expiry.
- `team list --pile PATH` — audit the pile: per-cap details (issuer → subject, scope, expiry — sorted soonest-expiry-first).
- `team list-pending --pile PATH [--author PUBKEY_HEX]` — display the observed, rejected, derived-pending, and complete issued-signature fact sets for incoming requests. Exactly one valid assertion author is auto-detected when omitted, without reading or creating a key; only a Complete view is shown.
- `team list-issued --pile PATH [--author PUBKEY_HEX]` — display every exact grant in one author's Complete asserted policy view, including the full `GrantDisabled` selector, disabled state, historical issuance, selected credential, authentication, and current usability. Author selection matches `list-pending`.
- `team show --pile PATH --cap HEX [--verify HEX] [--expected-subject HEX]` — walk one chain end-to-end and print each level with subject, issuer, scope, expiry, cap handle, proof position, and a signer-matches-issuer check. Bounded by MAX_DEPTH=32; the diagnostic deep-dive that complements `team list`'s summary view.

The daemon resolves its signing author's policy and recipient ledgers at one
coherent assertion boundary. It renews only the selected founder self grant and
remote-subject grants for the configured team. Enabled historical `Current`
values remain renewal seeds after expiry, but only live `usable_at(now)` winners
may operate or be sent. Each successor is asserted first and then selected
again from fresh durable truth. Founder self-rotation reconstructs the anchor
from the selected proof and reconciles host AUTH directly; ordinary grants are
delivered remotely and accepted through the recipient ledger.

### Work with remote stores

#### Blobs

- `store blob list <URL>` — list objects at a remote store.
- `store blob put <URL> <FILE>` — upload a file to a remote store and print its handle.
- `store blob get <URL> <HANDLE> <OUTPUT>` — download a blob from a remote store.
- `store blob forget <URL> <HANDLE>` — remove an object from a remote store.
- `store blob inspect <URL> <HANDLE>` — display metadata for a remote blob.

`ObjectStoreRemote` intentionally exposes only blobs. It does not claim the
coherent snapshot and durable-append contracts required
by `PinAssertionStore`: generic object-store listing is not a
point-in-time snapshot, and the advertised file backend exposes no directory
durability barrier. Remote assertion persistence and replication therefore need
an explicit assertion ledger rather than a scalar-HEAD or LIST-plus-GET shim.

## Development

Command implementations live in `src/cli/`, with assertion-native branch
operations under `pile`, plus `store` and team-management modules.
