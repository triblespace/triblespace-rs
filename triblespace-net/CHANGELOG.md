# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.38.0] - 2026-05-07

### Changed (breaking)
- **`PeerConfig.gossip_topic: Option<String>` →
  `PeerConfig.gossip: bool`.** The gossip topic is now derived
  from `team_root` directly (an ed25519 pubkey is already 32
  uniform bytes — perfect as a `TopicId`, no hashing needed),
  so users no longer pick + coordinate a separate topic
  string. One identifier per team handles both auth (cap
  chain) and rendezvous (gossip mesh) — no way to join the
  right team on the wrong gossip channel and silently see no
  HEAD updates.
  Migration: `gossip_topic: Some(_)` → `gossip: true`,
  `gossip_topic: None` → `gossip: false`.

## [0.37.0] - 2026-05-06

The auth-arc tests-and-polish release. No protocol changes —
pile-sync stays at v4 with the auth model from 0.36.0 — but
the testing surface and the runtime ergonomics matured
substantially.

### Added
- **End-to-end iroh auth tests un-ignored.** Three tests
  (smoke handshake + AUTH_OK + AUTH_REJECTED) pass green over
  real `TestNetwork` endpoints using the
  `/triblespace/pile-sync/4` ALPN. Catches QUIC-stream-level
  regressions that the lib-only tests miss. The lesson saved
  for future test authors: helpers must return `(router,
  endpoint, connection)` — dropping an iroh `Endpoint` tears
  down all its owned `Connection`s silently, so a helper
  returning only the `Connection` produces tests that fail
  for non-obvious reasons.
- **Runnable `Peer` doctest** showing the canonical
  construction shape (`PeerConfig { team_root, self_cap,
  peers, gossip_topic, revoked }`).

### Changed
- **Live revocation pickup** every `Peer::refresh` (auto-called
  on every read or write through the Peer). The update path
  rescans the snapshot for `(rev, sig)` blob pairs signed by
  the configured team root and unions them into the live
  revoked set. A revocation gossiped into the pile is
  therefore picked up on the next snapshot refresh — no
  relay restart.
- **Reachability BFS amortised across `OP_CHILDREN` responses.**
  The blob-level scope gate's reachability scan was previously
  recomputed per request; it's now cached across responses
  within one connection so a peer fetching many children pays
  the BFS once.
- **`PeerConfig` doc surface** points at `Peer::new` and
  records the deliberate "no `Default` impl" rationale (every
  construction site must specify a team root because auth is
  mandatory).

## [0.36.0] - 2026-04-28

The 0.36 line is the **chain-of-trust capability auth** release. Wire
protocol bumps to v4 with mandatory authentication on every connection;
the relay now enforces branch- and blob-level scope gates derived from
the verified cap. See `triblespace-rs/book/src/capability-auth.md` for
the user-facing chapter and the
[`triblespace_core::repo::capability`](https://github.com/triblespace/triblespace-rs/blob/main/triblespace-core/src/repo/capability.rs)
module for the auth-lib design rationale.

### Breaking
- **Pile-sync ALPN bumped to `/triblespace/pile-sync/4`.** Connections
  on `/3` are no longer accepted. v4 requires `OP_AUTH` as the first
  stream of every connection (presenting the caller's cap-sig handle)
  before any other op is served. Pre-v4 peers must upgrade.
- **`PeerConfig` no longer implements `Default`.** Every construction
  site must specify `team_root: VerifyingKey`, `revoked: HashSet<…>`,
  and `self_cap: RawHash`. The CLI's single-user fallback sets
  `team_root = signing_key.verifying_key()` and `self_cap = [0u8; 32]`,
  but library callers must opt in explicitly.

### Added
- **Server-side auth handler** (`SnapshotHandler`): on every incoming
  connection, the first stream is verified via
  `triblespace_core::repo::capability::verify_chain` against the
  configured `team_root`. Subsequent streams inherit the verified
  cap for the connection's lifetime; un-authed streams are silently
  closed.
- **Client-side `connect_authed`**: every outgoing op uses a single
  helper that does `ep.connect(...)` + `op_auth(self_cap)` so the
  auth round-trip is automatic on `Track`, `ListBranches`,
  `HeadOfRemote`, and `Fetch`.
- **Two-tier scope gate** in the protocol handler:
  - **Branch level** (`OP_LIST`, `OP_HEAD`): filtered by the verified
    cap's `granted_branches` set; out-of-scope branches are dropped
    from `OP_LIST` and surface as `NIL_HASH` on `OP_HEAD`.
  - **Blob level** (`OP_GET_BLOB`, `OP_CHILDREN`): a per-op
    reachability set (`reachable_set_for`) is computed once from the
    allowed branch heads via 32-byte-chunk BFS, and each candidate
    hash is checked for `O(1)` HashSet membership. Closes the
    raw-hash bypass that the branch-level gate alone left open.
- **Live revocation propagation**: `NetSender::update_snapshot`
  rescans every new snapshot for `(rev, sig)` blob pairs signed by
  the configured team root and unions them into the live
  `revoked: Arc<RwLock<HashSet<VerifyingKey>>>` shared with the
  handler. A revocation gossiped into the pile takes effect on the
  next snapshot refresh — no restart.
- **17 lib tests** in `host.rs::tests`:
  - Snapshot → verify_chain glue (3 tests)
  - Branch-level scope gating (3 tests, plus 1 admit-nothing edge case)
  - Blob-level reachability gate (3 tests)
  - Runtime revocation pickup (2 tests)
  - **End-to-end QUIC handshake** over iroh's `TestNetwork` custom
    transport (3 tests + 1 smoke echo): valid cap → AUTH_OK; zero
    cap → AUTH_REJECTED; foreign-root cap → AUTH_REJECTED.

### Changed
- `PeerConfig` gains `team_root: VerifyingKey`, `revoked: HashSet<…>`,
  `self_cap: RawHash` fields.
- `NetSender` carries the `team_root` and a shared `Arc<std::sync::RwLock<…>>`
  for the revoked set so `update_snapshot` can extend it from sync code
  while the async handler reads via brief read-clone-drop.
- `AnySnapshot` trait gains `all_simple_archive_blobs()` for the
  revocation rescan path.
- Lock primitive on `revoked` switched from `tokio::sync::RwLock` to
  `std::sync::RwLock` — sync-and-async accesses both safe (the async
  reads are read-clone-drop with no guard held across `.await`).

### Internal
- `dev-dependencies`: added `iroh = { features = ["test-utils",
  "unstable-custom-transports"] }` so the e2e auth handshake tests
  can run two endpoints on iroh's in-memory `TestNetwork` transport
  (no DNS, no relays, no IP). Plus `hifitime` for cap-expiry test
  helpers.
