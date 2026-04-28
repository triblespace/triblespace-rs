# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.36.0] - 2026-04-28
### Added
- `team` subcommand group for capability-based team management:
  - `team create` mints a fresh team root keypair, signs the founder's
    self-cap with admin scope, prints the team root pubkey + SECRET (to
    archive offline) + cap handles + the cap's expiry timestamp.
  - `team invite` issues a sub-cap to a teammate, delegating from the
    running node's own cap. Optional `--branch <BRANCH_HEX>` (repeatable)
    restricts the cap to specific branches.
  - `team revoke` issues a team-root-signed revocation against a pubkey,
    cascading transitively through any chain involving the revoked key.
  - `team list` audits the pile: per-cap details (issuer → subject,
    PERM_*/branches/expiry — sorted soonest-expiry-first) plus the
    `(revoker, target)` pairs for each verifiable revocation.
  - `team show --cap HEX` walks one chain end-to-end and prints
    each level (subject, issuer, scope, expiry, sig blob handle,
    cap blob handle, signer-matches-issuer check). Bounded by
    `MAX_DEPTH = 32`; embedded parent sigs at depth N>0 render
    with the `(embedded in level above)` label rather than a
    separate handle. The diagnostic deep-dive for "why is this
    cap rejected" — complements `team list`'s summary view.
  - `team show --verify <PUBKEY_HEX>` runs `verify_chain` against
    the given team root and prints `✓ VERIFIED` or `✗ FAILED —
    <VerifyError>`. The same code path the relay's `OP_AUTH`
    uses, so the result mirrors what a real connection attempt
    would see. Reads `TRIBLE_TEAM_ROOT` env var by default;
    `--expected-subject` overrides the default check (leaf cap's
    own subject) for subject-substitution-attack detection.
- `pile net sync` / `pile net pull` now read `TRIBLE_TEAM_ROOT` and
  `TRIBLE_TEAM_CAP` env vars for multi-user team operation; without them,
  fall back to single-user team-of-one (`team_root = signing_key.verifying_key()`).
- `pile net status` diagnostic subcommand: prints the node id, team
  root, and self_cap that the running peer would present on
  `OP_AUTH`, annotated with their source ("from TRIBLE_TEAM_ROOT"
  vs "single-user fallback" vs "NOT SET — remote will reject"). For
  debugging stuck-auth scenarios in one shot.
### Changed
- Pile-sync wire protocol bumped to v4 (`/triblespace/pile-sync/4`):
  every connection's first stream must be `OP_AUTH` presenting the
  caller's cap-sig handle; the server walks the chain back to the
  configured team root and either accepts (subsequent streams gated by
  the verified cap's scope) or rejects. Branch- and blob-level scope
  gates: `OP_LIST` / `OP_HEAD` filter by `granted_branches`, while
  `OP_GET_BLOB` / `OP_CHILDREN` reject blobs outside the reachable set
  from allowed heads (closes the raw-hash bypass).
- `team list`'s revocations section surfaces full `(revoker, target)`
  pairs, not just a count.

## [0.35.0] - 2026-04-18
### Added
- `pile branch reflog` command to list historical branch head updates (including tombstones) stored in a pile file.
- `pile branch journal` command to scan a pile for all branch update/tombstone records and report the latest state per branch id.
- `pile branch set` command to CAS-update a branch head to a specific branch-metadata handle (useful for recovery).
- `pile diagnose locate-hash` to scan raw pile bytes and report where a blob handle appears (header vs payload references).
- `pile net {identity, sync, pull}` subcommands for distributed pile sync over iroh (gossip + DHT + QUIC), built on `triblespace-net`.
### Changed
- `pile diagnose` is now a subcommand group (`check`, `locate-hash`) instead of a single command.
- `pile branch stats` now defaults to a fast path that reports accumulated content bytes and accumulated triple count from blob metadata (`length / 64`) without materializing commit payload tribles.
- `pile branch stats --full` retains the previous deep scan behavior for unique triples/entities/attributes.
- `trible` now lives in the `triblespace-rs` workspace as a first-class member, sharing the `Cargo.lock` with `triblespace-core` / `triblespace-net` / the facade. Dev experience: a single `cargo test --workspace` run exercises CLI, library, and protocol tests together.
### Fixed
- `pile create` now explicitly touches the target path before `Pile::open`. `Pile::open` stopped auto-creating files at triblespace-core 0.32.1 but the CLI hadn't been updated to match; `pile create` on a fresh path returned `No such file or directory`.
- `branch pull` does the same touch-before-open when the local pile is a fresh path.

## [0.12.0] - 2026-02-09
### Changed
- Updated CLI dependencies and replaced `rand` with `getrandom` for generating random ids and ephemeral signing keys.
- `pile branch create` now mints branch ids with `genid` (high-entropy random ids) instead of `ufoid` (time-prefixed ids).

## [0.11.1] - 2026-02-08
### Changed
- `pile branch list` now prints `id head name` for easier column alignment.

## [0.11.0] - 2026-02-08
### Added
- Initial changelog with Let's Changelog format.
- `pile merge` command to merge source branch heads into a target branch.
- Integration tests for `genid` and `pile list-branches` commands.
- `pile create` command to initialize new pile files.
- Note that `touch` on Unix can also create an empty pile file.
- `pile put` command for ingesting a file into a pile.
- `pile put` now memory maps the input for efficient ingestion.
- `pile get` command to extract blobs from a pile by handle.
- `pile blob inspect` command to show blob metadata like timestamp and size.
- `pile list-blobs` command to enumerate blob handles in a pile.
- `pile list-blobs` output now uses built-in `Hash` formatting.
- `pile diagnose` command to check pile integrity.
- `pile diagnose` now verifies that all blob hashes match.
- `pile diagnose` now exits with a nonzero code when corruption is detected.
- `pile migrate` command to apply idempotent pile metadata migrations.
- `pile migrate ... branch-metadata-name` migration to upgrade legacy branch-name metadata to `metadata::name` (LongString handle).
- `store blob list` command to enumerate object store contents.
- `store blob put` command to upload files to object stores.
- `store blob forget` command to remove objects from object stores.
- `store blob inspect` command to display metadata for remote blobs.
- `store blob get` command to download blobs from object stores.
- `store branch list` command to list branches in an object store.
- `pile branch create` command to create a new branch.
- `pile branch delete` command to delete a branch via a tombstone record.
- `branch push` and `branch pull` commands to sync branches with remote stores.
- Tests for branch creation and branch push/pull using a file object store.
- Logged an inventory task to provide a structured command overview in the README.
- Structured command overview in the README.
- Logged inventory tasks for inspection utilities, shell completions, progress reporting, and migrating to the published `tribles` crate.
- Renamed the future `store delete` command to `store forget` in the inventory.
- Step-by-step quick-start example in the README.
- `completion` command to generate shell scripts for bash, zsh, and fish.
- Test ensuring `pile blob list` outputs the exact handle for ingested blobs.
- Optional metadata output for `pile blob list`.
### Changed
- Versioning is now aligned with `triblespace` releases.
- Updated consolidate E2E test commits to pass optional metadata explicitly.
- Renamed `id-gen` command to `genid` to align with the GenID schema.
- Expanded `AGENTS.md` with sections from the Tribles project and a dedicated
  inventory subsection.
- Expanded crate metadata with additional keywords and categories.
- Removed explanatory comment about crate metadata from `Cargo.toml`.
- Increased default maximum pile size to 16 TiB.
- Fixed `pile put` compilation issues when using memmap.
- Renamed `pile pull` to `pile get` to avoid confusion with repository commands.
- Reworded inventory note about import/export commands to clarify blob
  transfers to piles and object stores via dedicated subcommands.
- Simplified `Pile::open` error handling now that `OpenError` implements
  `std::error::Error` upstream.
- `pile list-blobs` output uses lowercase hex instead of uppercase.
- `pile branch list` output now includes name and head commit in addition to the branch id.
- Pile commands reorganized under `branch` and `blob` subcommands.
- Store commands reorganized under `branch` and `blob` subcommands.
- Simplified file ingestion using `anybytes::Bytes::map_file` and removed
  the `memmap2` dependency.
- Split CLI command groups into modules under `src/cli`.
- Organized pile and store command implementations into submodules matching the CLI hierarchy.
- Consolidated pile-only branch commands under the `pile branch` subcommand.
- Rewrote README with a friendlier tone and clarified command list.
- Corrected pile file extension in README quick-start example.
- Deduplicated blob handle parsing across CLI modules.
- `pile blob put` and `store blob put` now print the blob handle after
  ingestion.
- Split CLI integration tests into smaller modules for readability.
- `pile create` now creates parent directories if they do not exist.
- Updated to latest `tribles` crate and imported required store traits.
### Removed
- Completed work entries have been trimmed from `INVENTORY.md` now that they are
  tracked here.
- Removed completed inventory item for crate metadata expansion.
- Removed inventory note for shell completions now that the feature exists.
- Removed note from README suggesting `touch` can create empty piles.
- Removed inventory entry for the old `diagnose` command now that the feature is
  implemented.
- Removed inventory item for the `pile list-blobs` command now that the feature
  exists.
- Removed inventory note for the `store blob forget` command now that the feature
  exists.
- Removed inventory notes for `store blob get` and `store blob inspect` now that those commands are implemented.
- Removed inventory note about `anybytes` helper integration.
- Removed stray `.orig` backup files from `src` and `tests` directories.
- Removed inventory note for a README quick-start example now that the section exists.
- Removed inventory note about offering an option for the `completion` command to write scripts directly to a file.
- Removed inventory entry for enhancing `pile blob list` with optional filtering.
