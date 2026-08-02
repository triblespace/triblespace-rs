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
- `pile migrate <PILE> list` — list known migrations and whether they are needed for this pile.
- `pile migrate <PILE> run [MIGRATION]` — run migrations (all by default). Pass `--dry-run` to preview changes.

If branch names are missing in an older pile, run:

```bash
trible pile migrate <PILE> run branch-metadata-name
```

#### Branches

- `pile branch assert <PILE> <NAME> <COMMIT> --signing-key <KEY>` — durably publish one assertion for a locally present canonical commit.
- `pile branch list <PILE> [--signing-key <KEY> | --author <AUTHOR> | --all]` — list exact identities and their local `complete`, `partial`, or `tip-pending` resolution.
- `pile branch show <PILE> <BRANCH>` — inspect one exact descriptor, its assertions, frontier, missing ancestry, and derived read head.
- `pile branch log <PILE> <BRANCH>` — walk locally available ancestry from the resolver's candidate tips.
- `pile branch forget <SOURCE> <DESTINATION> <BRANCH>` — create a new local pile generation without that exact identity's assertion records. This is physical forgetting, not replicated deletion; the source remains untouched and synchronization can reintroduce the assertions.

`<BRANCH>` is always the full descriptor
`ed25519:<64 hex>/blake3:<64 hex>`. The truncated 16-byte branch id printed as
`index=…` is advisory only and is never accepted as a selector. Empty branches,
mutable scalar heads, rename, consolidation, and replicated deletion are not
part of the StrongPin model.

Signing key format
- `branch assert` requires a stable signing-key file via `--signing-key` or the `TRIBLES_SIGNING_KEY` path. The file contains one 64-character hex Ed25519 seed. Commands never invent an ephemeral branch author.

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
- `pile net status [--key PATH]` — print the auth configuration this node would present on `OP_AUTH`: node id, team root, self_cap, and where each value comes from (env var vs fallback). For debugging stuck-auth scenarios.
- `pile net sync <PILE> [--peers ID,...] [--key PATH]` — announce local blobs and service durable weak-pin wants over the team network. Reads `TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP`; the team root falls back to the node's own pubkey for a team-of-one, while a missing cap leaves the node recovery/server-only. `--no-lazy` disables want reconciliation. Signed-assertion replication still requires a dedicated protocol and admission policy.

### Capability auth

Chain-of-trust capability system for distributed pile sync. A team has one
immutable root keypair, used once at creation and then archived, which signs a
non-expiring founder anchor. The founder's finite operational credential and
future rotations are siblings beneath that anchor; ordinary capabilities
continue by delegation. See
[`book/src/capability-auth.md`](../book/src/capability-auth.md) for
the full design.

- `team create --pile PATH [--key KEY_PATH]` — mint a team root, sign one non-expiring founder anchor, issue a finite founder operational cap beneath it, and durably pin the complete credential. Prints the public team root, root secret to archive offline, anchor and operational handles, and operational expiry.
- `team invite --pile PATH --team-root HEX --cap HEX --key ISSUER --invitee HEX --scope (read|write|admin) [--legacy-pin HEX]...` — issue a sub-capability to another peer. ISSUER must hold a cap that subsumes the requested scope. `--legacy-pin` (repeatable) restricts the current blob RPC to reachability from mutable local-pin roots; it does not select an exact StrongPin branch.
- `team request-join --admin HEX --scope (read|write|admin) [--key PATH] [--pile PATH]` — send an `OP_REQUEST_CAP` to an admin asking to be issued a capability via the running auth-handshake daemon.
- `team approve --pile PATH --entry HEX --team-root HEX --cap HEX [--key PATH]` — approve a pending join request, sign the cap, dispatch it via the auth-handshake ALPN, and add a renewal-policy entry so the cap stays renewed.
- `team retract --pile PATH --entry HEX` — stop auto-renewing a (subject, scope) entry. The peer's cap chain dies at its next natural expiry. Pure local decision, takes effect on the next daemon tick. There is no team-root broadcast revocation primitive; eviction is per-issuer non-renewal.
- `team list --pile PATH` — audit the pile: per-cap details (issuer → subject, scope, expiry — sorted soonest-expiry-first).
- `team list-pending --pile PATH` — incoming join requests awaiting approval.
- `team list-issued --pile PATH` — renewal-policy entries this node is keeping renewed.
- `team show --pile PATH --cap HEX [--verify HEX] [--expected-subject HEX]` — walk one chain end-to-end and print each level with subject, issuer, scope, expiry, cap handle, proof position, and a signer-matches-issuer check. Bounded by MAX_DEPTH=32; the diagnostic deep-dive that complements `team list`'s summary view.

### Work with remote stores

#### Blobs

- `store blob list <URL>` — list objects at a remote store.
- `store blob put <URL> <FILE>` — upload a file to a remote store and print its handle.
- `store blob get <URL> <HANDLE> <OUTPUT>` — download a blob from a remote store.
- `store blob forget <URL> <HANDLE>` — remove an object from a remote store.
- `store blob inspect <URL> <HANDLE>` — display metadata for a remote blob.

#### Mutable legacy pins

- `store pin list <URL>` — list every replica-local mutable pin id at an object-store URL. This is an unclassified storage view and can include old content-branch heads as well as local policy or retention pins.

`ObjectStoreRemote` intentionally exposes only blobs and replica-local pins.
It does not claim the coherent snapshot and durable-append contracts required
by a StrongPin assertion store: generic object-store listing is not a
point-in-time snapshot, and the advertised file backend exposes no directory
durability barrier. Remote assertion persistence and replication therefore need
an explicit assertion ledger rather than a scalar-HEAD or LIST-plus-GET shim.

## Development

Command implementations live in `src/cli/`, with assertion-native branch
operations under `pile`, plus `store` and team-management modules.
