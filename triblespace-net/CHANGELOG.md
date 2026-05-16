# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.41.1] - 2026-05-17

### Changed (breaking — public API)
- **`Peer::{track, pull_branch, list_remote_branches, fetch,
  head_of_remote}` and `resolve_branch_name`** now take
  `impl Into<EndpointAddr>` instead of bare `EndpointId`.
  Source-compatible for `EndpointId` callers via the standard
  `Into<EndpointAddr>` impl; new callers can pass a full
  `EndpointAddr` (with relay URL + direct addresses) to
  bypass iroh's discovery layer in environments where pkarr
  publish / relay probes are blocked.

- **`NetCommand::{Track, ListBranches, HeadOfRemote, Fetch}`**
  carry `EndpointAddr` instead of `EndpointId` on the wire.

- **`connect_authed`** + the private `fetch_blob`,
  `fetch_reachable`, `track_known_head` helpers in `host.rs`
  take `EndpointAddr`, threading address info down to iroh's
  `Endpoint::connect` so it can dial directly without
  resolving via discovery.

### Added
- **Rich `EndpointTicket` print at sync startup.**
  `host_loop` calls `ep.addr()` after `ep.online()` returns
  and writes a `ticket: …` line to stderr containing the
  full `EndpointAddr` (id + relay URL + direct addresses) as
  a standard iroh `EndpointTicket`. This is the form to paste
  into another peer's `--peers` flag for direct dial.

- `iroh-tickets 0.5` dependency for the ticket
  serialization.

## [0.41.0] - 2026-05-16

### Changed (breaking — transitive)
- **Iroh family bumped 0.97 → 0.98** (`iroh`, `iroh-base`,
  `iroh-gossip`) plus `iroh-blobs` 0.99 → 0.100, `irpc` 0.13
  → 0.14, `irpc-iroh` 0.13 → 0.14.

  Replaces the 0.40.3 Cargo.lock-shipping workaround with a
  real fix: `iroh-base 0.97`'s `=3.0.0-pre.1` pin on
  `ed25519-dalek` no longer compiles against
  `ed25519 v3.0.0` (released 2026-05-03 — `KeyMalformed`
  changed from unit to tuple variant). `iroh-base 0.98`
  re-pins to `=3.0.0-pre.6`, which is API-compatible.

  No surface API changes for consumers — `PeerConfig`, the
  `Peer` type, and the protocol handler stay the same.
  Iroh's `Endpoint::builder`, `presets::N0`,
  `CaRootsConfig::system()`, and `ProtocolHandler` all kept
  their shape across the iroh minor bump.

  Verified: 17 lib + 2 + 3 integration tests + the e2e auth
  handshake suite over `TestNetwork` all pass; `cargo install
  trible --locked` from the 0.41.0 workspace succeeds without
  the lockfile workaround.

## [0.40.2] - 2026-05-16

### Fixed
- **TLS roots now come from the OS trust store** via
  `rustls-platform-verifier`, instead of the compiled-in Mozilla
  `webpki-roots` bundle. The `platform-verifier` feature on the
  iroh dep is enabled, and `host.rs` calls
  `Endpoint::builder(...).ca_roots_config(CaRootsConfig::system())`.

  Why: corporate-proxy / sandbox environments (e.g. Anthropic
  web-sandbox egress) present a custom CA at TLS interception.
  webpki-roots is a frozen Mozilla snapshot and ignores the OS
  store, so iroh's relay HTTPS probes and pkarr publish/lookup
  fail with `invalid peer certificate: UnknownIssuer`, discovery
  dies silently, and the QUIC peer handshake never starts.
  Reading the OS store at runtime lets admin-installed roots
  (and the sandbox CA) participate. macOS uses the Security
  framework, Linux reads `/etc/ssl/certs`, Windows reads the
  certificate store. Standard Mozilla roots remain trusted on
  all three since they're already in the OS store.

  Reported and diagnosed by another Claude instance running in
  the Anthropic web sandbox after seeing the
  `WARN [...] UnknownIssuer` lines from the new tracing
  surface — exactly the kind of failure the previous
  Unreleased tracing-instrumentation work was supposed to
  surface, and did.

### Added
- **Tracing instrumentation across the auth handshake and op
  surface.** `SnapshotHandler::accept` opens an `info`-level
  `connection` span (`peer`, `alpn`); each `serve_stream` call
  enters a `debug`-level `stream` span carrying the op name
  (`AUTH`/`LIST`/`HEAD`/`GET_BLOB`/`CHILDREN`). Auth events fire
  at `info` (auth ok, granted-branch count, unrestricted flag)
  or `warn` (auth rejected with the inner `VerifyError` reason,
  peer-pubkey-parse failure). Per-op events log at `debug` for
  normal traffic and `warn` on scope-deny so out-of-scope
  branch / blob requests surface immediately.
- **Stream span duration = op latency** by construction —
  subscribers that record span timings (`tracing-subscriber`'s
  `FmtSpan::CLOSE`, `tracing-flame`, `tracing-opentelemetry`,
  Tokio Console) get per-op latency observability without
  further instrumentation.
- **Client-side `connect_authed` is now a `info`-level span**
  with `peer` field; emits structured `warn` events on
  connect failure and auth-handshake failure with the inner
  error preserved.

### Changed
- The 12 `eprintln!("[net] …")` ad-hoc log calls in
  `host.rs` (gossip neighbor up/down, hash-mismatch warnings,
  fetch errors, the catastrophic bind/connect failures, the
  stream handler error) are converted to `tracing` events at
  appropriate levels (`info` for normal lifecycle,
  `warn` for protocol-level anomalies, `error` for
  thread-fatal failures). The `[net]` prefix is dropped — the
  subscriber handles formatting.
- The two remaining stray `eprintln!`s in `identity.rs` (the
  one-time "generated new node key" notice on first startup)
  and `tracking.rs` (the stale-tracking-update skip
  diagnostic) are also converted to `tracing` events
  (`info` and `debug` respectively). `triblespace-net`'s
  `src/` tree no longer contains any `eprintln!`s — every
  diagnostic surface now flows through the subscriber.

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
