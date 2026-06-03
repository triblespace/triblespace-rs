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

- `pile create <PATH>` — initialize an empty pile, creating parent directories as needed.
- `pile diagnose check <PILE>` — verify pile integrity.
- `pile diagnose locate-hash <PILE> <HANDLE>` — scan raw pile bytes and report where a handle appears (blob header vs payload references).
- `pile migrate <PILE> list` — list known migrations and whether they are needed for this pile.
- `pile migrate <PILE> run [MIGRATION]` — run migrations (all by default). Pass `--dry-run` to preview changes.

If branch names are missing in an older pile, run:

```bash
trible pile migrate <PILE> run branch-metadata-name
```

#### Branches

- `pile branch list <PILE>` — list branch ids, heads, and names.
- `pile branch create <PILE> <NAME>` — create a new branch.
- `pile branch delete <PILE> <BRANCH_ID>` — delete a branch (writes a tombstone record).
- `pile branch stats <PILE> <BRANCH_ID>` — fast branch stats (commit count + accumulated content blob bytes + accumulated triple count via `bytes / 64`).
- `pile branch stats <PILE> <BRANCH_ID> --full` — additionally materialize content to compute unique triples/entities/attributes (slower).
- `pile branch consolidate <PILE> <BRANCH_ID...>` — consolidate multiple branches into a single new branch. The command creates a single merge commit whose parents are the selected branch heads and prints the new branch id.
- `pile merge <PILE> <TARGET_ID> <SOURCE_ID...>` — merge source branch heads into a target branch by creating merge-only commits.

Signing key format
- Commands that create commits (e.g. `create`, `merge`, `merge-import`, `consolidate`) accept a signing key file path via the `--signing-key` flag. The file must contain a single 64-character hex string (32 bytes encoded as hex). You can also set TRIBLES_SIGNING_KEY to the path of such a file. Generated keys (when created by Codex tooling) are written as hex text to the configured path.

#### Blobs

- `pile blob list [--metadata] <PILE>` — list stored blob handles. Pass `--metadata` to include timestamps and sizes.
- `pile blob put <PILE> <FILE>` — store a file as a blob and print its handle.
- `pile blob get <PILE> <HANDLE> <OUTPUT>` — extract a blob by handle.
- `pile blob inspect <PILE> <HANDLE>` — display metadata for a stored blob.

### Distributed pile sync

Built on `triblespace-net` (iroh QUIC + DHT + gossip). All commands
authenticate via capability chains rooted at a team's pubkey; see
the *Capability auth* section below for the team setup. Without a
team configured, falls back to single-user team-of-one (the user is
their own team root).

- `pile net identity [--key PATH]` — print this node's iroh identity (auto-generates a key if missing).
- `pile net status [--key PATH]` — print the auth configuration this node would present on `OP_AUTH`: node id, team root, self_cap, and where each value comes from (env var vs fallback). For debugging stuck-auth scenarios.
- `pile net sync <PILE> [--peers ID,...] [--key PATH]` — long-running bidirectional sync on the team's gossip mesh. The mesh is identified by the team root pubkey directly (no separate topic argument): every team has exactly one mesh, derived from its identity. Auto-merges incoming tracking branches into same-named local ones every tick. Reads `TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP` env vars; falls back to the node's own pubkey for single-user / team-of-one workflows.
- `pile net pull <PILE> <REMOTE> --branch NAME [--key PATH]` — one-shot pull of a named branch from a specific peer (REMOTE is the peer's iroh node id, 64-char hex). Pull-only mode — no gossip subscription, direct QUIC + DHT fetch, materializes a tracking branch and merges into local. Same env-var fallback as `sync`.

### Capability auth

Chain-of-trust capability system for distributed pile sync. A team
has one immutable root keypair (used once at creation, then archived)
that signs the founder's capability; every other capability chains
off the founder's via delegation. See
[`book/src/capability-auth.md`](../book/src/capability-auth.md) for
the full design.

- `team create --pile PATH [--key KEY_PATH]` — mint a new team root keypair, sign the founder's self-cap with admin scope, and write both into the pile. Prints the team root pubkey (publish to peers), team root SECRET (archive offline), founder cap handles, and the cap's expiry timestamp.
- `team invite --pile PATH --team-root HEX --cap HEX --key ISSUER --invitee HEX --scope (read|write|admin) [--branch HEX]...` — issue a sub-capability to another peer. ISSUER must hold a cap that subsumes the requested scope. `--branch` (repeatable) restricts the cap to specific branches; without it the cap applies to every branch within the granted permission set.
- `team request-join --admin HEX --scope (read|write|admin) [--key PATH] [--pile PATH]` — send an `OP_REQUEST_CAP` to an admin asking to be issued a capability via the running auth-handshake daemon.
- `team approve --pile PATH --entry HEX --team-root HEX --cap HEX [--key PATH]` — approve a pending join request, sign the cap, dispatch it via the auth-handshake ALPN, and add a renewal-policy entry so the cap stays renewed.
- `team retract --pile PATH --entry HEX` — stop auto-renewing a (subject, scope) entry. The peer's cap chain dies at its next natural expiry. Pure local decision, takes effect on the next daemon tick. There is no team-root broadcast revocation primitive; eviction is per-issuer non-renewal.
- `team list --pile PATH` — audit the pile: per-cap details (issuer → subject, scope, expiry — sorted soonest-expiry-first).
- `team list-pending --pile PATH` — incoming join requests awaiting approval.
- `team list-issued --pile PATH` — renewal-policy entries this node is keeping renewed.
- `team show --pile PATH --cap HEX [--verify HEX] [--expected-subject HEX]` — walk one chain end-to-end and print each level with subject, issuer, scope, expiry, blob handles, and a signer-matches-issuer check. Bounded by MAX_DEPTH=32; the diagnostic deep-dive that complements `team list`'s summary view.

### Work with remote stores

#### Blobs

- `store blob list <URL>` — list objects at a remote store.
- `store blob put <URL> <FILE>` — upload a file to a remote store and print its handle.
- `store blob get <URL> <HANDLE> <OUTPUT>` — download a blob from a remote store.
- `store blob forget <URL> <HANDLE>` — remove an object from a remote store.
- `store blob inspect <URL> <HANDLE>` — display metadata for a remote blob.

#### Branches

- `store branch list <URL>` — list branches at a remote store.
- `branch push <URL> <PILE> <ID>` — push a branch to a remote store.
- `branch pull <URL> <PILE> <ID>` — pull a branch from a remote store.

See `INVENTORY.md` for notes on possible cleanup and future functionality.

## Development

Command implementations live in `src/cli/` with modules for `branch`, `pile`,
and `store`. The modules expose their subcommands and are re-exported from
`main.rs` to preserve the existing CLI interface. Contributions are always
welcome!
