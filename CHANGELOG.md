# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.42.5] - 2026-05-18

### Fixed
- **`Peer::new` startup-sweep race.** The sweep iterated blobs from
  one `store.reader()` snapshot and captured the diff baseline from
  a second `store.reader()` call. An external append (e.g. `trible
  team invite` writing a cap blob to the pile file) landing between
  the two reads slipped into the baseline without ever being
  announced — the blob then was locally present but invisible to
  `find_providers` DHT lookups. Symptom: cap-chain swarm-fetch
  fallback failing because the cap-holder appeared to be the only
  provider in the DHT (the actual minter never announced).

  Fix: start with `last_blob_reader = None`. The first refresh
  announces every blob in `current` directly (no diff), then
  captures baseline. Single `reader()` call, no race. `Peer::new`
  drives one synchronous refresh before returning so the DHT
  learns about pre-existing blobs before the first incoming AUTH
  can land.

## [0.42.4] - 2026-05-18

Stale-update gate replaced by storage-layer idempotency.

### Changed
- **`Pile::update` short-circuits no-op writes.** When the
  requested head equals the current head, `Pile::update`
  returns `PushResult::Success` without appending a record.
  The branch table is logically an `(id → head)` map; a write
  where `new == current` carries no information and would
  just churn the append-only file. Steady-state gossip
  rebroadcasts of unchanged heads (tracking-branch
  re-publication at 30s ticks) hit this path heavily.

### Removed
- **Wall-clock stale-update gate in
  `triblespace_net::tracking::update_tracking_branch`.** The
  gate compared `metadata::updated_at` of the incoming
  gossip to the tracking branch's stamp and rejected
  not-strictly-newer updates. With the storage-layer
  idempotency above, identical heads collapse to a no-op
  inside `Pile::update`; semantically different out-of-order
  heads are reconciled downstream by `Workspace::merge_commit`'s
  ancestry check (no-op if remote is already in local's
  ancestry, fast-forward if local is in remote's ancestry,
  merge commit otherwise). The wall-clock comparison was
  redundant.

## [0.41.4] - 2026-05-17

The two follow-ons surfaced by the first successful sandbox sync.

### Fixed
- **Trailing-dot leak through `ep.addr()`.** 0.41.3 stripped
  dots from the *outbound* RelayMap, so our local relay
  connect path was clean. But iroh's `Endpoint::addr()` can
  still return an `EndpointAddr` whose `TransportAddr::Relay`
  carries the dotted form (the relay server reports its
  canonical URL back to the client and iroh stores that for
  its own-address reporting). When we serialise that
  EndpointAddr into a ticket via `pile net sync` startup,
  the dotted URL propagates to whoever consumes the ticket;
  their iroh then dials us via the dotted URL and trips
  WAFs on their egress.

  Adds `triblespace_net::dot_stripped_endpoint_addr(addr)` —
  a normaliser applied at every channel boundary that emits
  or consumes an `EndpointAddr`. Used in
  `triblespace-net`'s ticket print + in `trible`'s
  `parse_peers` and `pile net pull <REMOTE>` parsing, so
  outbound tickets are dot-free and inbound tickets get
  normalised even when minted by an unpatched peer.

- **Connection-per-RPC stall in `fetch_reachable`.**
  Previously the BFS over a remote pile opened a fresh
  `connect_authed` for every `op_children` parent and every
  `op_get_blob` child. Each auth handshake costs roughly
  600ms (TLS + QUIC + OP_AUTH round trip + `verify_chain`),
  so even a small remote pile of ~30 blobs would exhaust
  the `pull_branch` 30-second deadline before the walk
  completed.

  `fetch_reachable` now opens **one** authed connection at
  the top of the function and reuses it for every
  `op_children` and `op_get_blob` call along the BFS.
  iroh's QUIC multiplexes streams cheaply, and our
  `SnapshotHandler::accept` already accepts multiple
  sequential bi-streams per connection — auth state is
  per-connection, set on the first OP_AUTH stream, and
  reused on every subsequent stream.

  The previous DHT-fallback path that lived in the
  per-blob `fetch_blob` helper is dropped from this hot
  path; DHT reachability hasn't been load-bearing for any
  current use case and adding a per-blob connect to a
  different peer would defeat the reuse. The standalone
  `fetch_blob` helper is still used by the single-blob
  `NetCommand::Fetch` RPC path.

  Net effect: a remote-pull walk that took 39+ connections
  on 0.41.3 now takes 1. The previously-observed "connect
  → auth_ok → LocallyClosed → reconnect" cycle disappears.

### Notes
- Diagnosed by the same other-Claude instance — the
  diagnostic surface from the tracing instrumentation
  continues to pay off.
- File-upstream candidate: iroh's `RelayUrl::parse` could
  normalise trailing dots, which would let us drop both
  workarounds. The full-completeness fix is in iroh.

## [0.41.3] - 2026-05-17

The trailing-dot fix. The reason iroh's HTTPS probes to the
default relays were getting 503'd from the Anthropic web
sandbox — and likely from any other corporate environment
fronted by a strict WAF.

### Fixed
- **iroh default relay hostnames had a trailing FQDN dot** —
  `iroh-0.98.2/src/defaults.rs` ships e.g.
  `"use1-1.relay.n0.iroh-canary.iroh.link."` (note the
  trailing dot, the DNS absolute-form marker). When iroh
  builds an HTTPS probe URL from those, the dot propagates
  into reqwest's `Host` header. WAFs that treat trailing-dot
  Host as a known bypass-attempt signature reject those
  requests with synthetic 503s, leaving iroh's `net_report`
  cycle permanently stuck and — in iroh's current connect
  design — preventing direct-dial attempts that would
  otherwise honor a ticket's pre-known addresses.

  triblespace-net now transforms iroh's prod default relay
  map at endpoint-build time, stripping the trailing dot from
  each relay's hostname before iroh constructs the `RelayUrl`s.
  Same upstream relay (DNS doesn't care about
  absolute/relative-form distinction); HTTP-canonical Host
  header on the wire.

  Diagnosed by another Claude instance in the web sandbox via
  an exhaustive narrowing experiment that ruled out User-Agent
  (`reqwest/0.12.x` works), TLS fingerprint (vanilla rustls
  +reqwest+native-roots works), burst rate (20× concurrent
  curls all 200), HTTP version, and headers — then nailed it
  with a side-by-side comparison: identical rustls-reqwest
  probes succeeded 20/20 in the same second iroh's own
  probes got 12/12 503'd. The smoking gun was the URL form
  iroh logged: `https://...iroh.link./` (dot before slash).

  Fix transforms `iroh::defaults::prod::default_relay_map()`
  rather than hardcoding hostnames, so we stay in sync with
  whatever n0 ships. Filed upstream-fix candidate: have iroh
  normalize trailing dots in `RelayUrl::parse` or its
  hostname constants. Until that lands, this is the
  triblespace-side workaround.

## [0.41.2] - 2026-05-17

The address-symmetry release. Closes the
"tickets-work-for-pull-but-not-sync" asymmetry from 0.41.1
by seeding iroh's address lookup with bootstrap-peer
addresses, so the gossip mesh / DHT bootstrap path can dial
ticket peers directly — no pkarr/DNS roundtrip.

### Added
- **`triblespace-net::address_lookup::StaticAddressLookup`**:
  an `iroh::address_lookup::AddressLookup` implementation
  seeded with a fixed `EndpointId → EndpointAddr` map at
  construction. Hooked into the endpoint via
  `Builder::address_lookup(static_lookup)`; layered alongside
  the `presets::N0` pkarr+DNS lookups (lookup services are
  additive on the iroh builder). For known peers, returns
  the cached `EndpointAddr` immediately; for unknown peers,
  yields an empty stream so the other registered lookup
  services get their turn.

### Changed (breaking — public API)
- **`triblespace_net::peer::PeerConfig.peers`** is now
  `Vec<EndpointAddr>` (was `Vec<EndpointId>`).
  Source-compatible for `EndpointId` callers via the
  standard `EndpointId: Into<EndpointAddr>` impl
  (`peers: vec![id.into()]`).

  Callers passing an `EndpointTicket` through
  `pile net sync --peers <STR>` now get a real address
  benefit on the gossip + DHT bootstrap path:
  iroh-gossip's `JoinOptions::bootstrap` still takes
  `Vec<EndpointId>`, but iroh's connect goes through
  `AddressLookup` to resolve the id, and our static
  provider answers immediately with the ticket's addresses.

### Fixed
- **`pile net sync` direct-dial in sandbox / restricted-
  network environments.** Previously the gossip mesh
  bootstrap path needed iroh discovery to resolve peer
  addresses; in environments where pkarr publish or DNS
  are blocked (Anthropic web sandbox, corporate proxies,
  etc.) gossip silently couldn't connect even when
  `--peers <EndpointTicket>` carried the addresses. With
  the static lookup seeded from `PeerConfig.peers`, gossip
  bootstrap now succeeds.

## [0.41.1] - 2026-05-17

The `EndpointTicket`-everywhere release. Makes sandbox /
corporate-proxy environments actually able to dial peers
without going through iroh discovery — the missing piece
behind the v0.41.0 / faculties v0.14.4 round of testing.

### Changed (breaking — public API of `triblespace-net::peer`)

- **`Peer::track`, `Peer::pull_branch`, `Peer::list_remote_branches`,
  `Peer::fetch`, `Peer::head_of_remote`, and the free function
  `resolve_branch_name`** now take `impl Into<EndpointAddr>`
  instead of bare `EndpointId`.

  Source-compatible for existing callers passing `EndpointId`
  (the `Into<EndpointAddr>` impl is automatic). Lets new
  callers pass a full `EndpointAddr` — carrying the relay URL
  and direct socket addresses — through to iroh's
  `Endpoint::connect`, which honours those addresses and
  skips discovery entirely.

  Why this matters: discovery is broken in many real
  environments. claude.ai's web sandbox is a Firecracker
  microVM behind a TLS-intercepting egress with a shared IP
  rate-limited by iroh-canary; corporate networks block pkarr
  publish; some restricted CI environments block UDP entirely.
  In all these cases, `Endpoint::connect(EndpointAddr, ALPN)`
  with the addresses pre-filled in the `EndpointAddr`
  succeeds where the discovery-resolved path fails silently.

- **`NetCommand::Track`, `NetCommand::ListBranches`,
  `NetCommand::HeadOfRemote`, `NetCommand::Fetch`** carry
  `EndpointAddr` instead of `EndpointId` on the wire from
  `NetSender` to `host_loop`. Internal but listed here for
  anyone implementing the channel directly.

- **`fetch_blob`, `fetch_reachable`, `track_known_head`,
  `connect_authed`** (private helpers in `host.rs`) take
  `EndpointAddr` so address info flows through to the QUIC
  layer. Callers with only an `EndpointId` use
  `EndpointAddr::from(id)` (no addresses → discovery fallback,
  same behaviour as before).

### Added

- **`pile net sync` prints an `EndpointTicket`** to stderr
  once the iroh endpoint is online — the rich form encoding
  `node_id + relay URL + direct addrs`. This is what to copy
  into a peer's `--peers` flag for direct dial in
  discovery-hostile environments. Printed via `eprintln`
  (not just tracing) so it shows at default log levels.

- **`pile net identity` prints an `EndpointTicket`** alongside
  the bare pubkey. Without a running endpoint this carries
  only the id (no addresses); use the richer ticket from
  `pile net sync` startup for direct-dial scenarios.

- **`pile net pull <REMOTE>` accepts an `EndpointTicket`** as
  the `<REMOTE>` argument in addition to the legacy bare-
  pubkey form. Backward-compatible.

- **`pile net sync --peers <STR>` accepts `EndpointTicket`s**
  in addition to bare hex pubkeys. Mixed lists are fine.
  Tickets are decoded to `EndpointAddr`; for the gossip
  bootstrap path the id is extracted (the address info is
  not yet used to seed iroh's address cache for gossip, but
  the address info IS used end-to-end for the
  `pile net pull` path).

### Notes

- The `pile net sync` gossip bootstrap doesn't yet seed iroh's
  address cache from ticket addresses, so sandbox-side `sync`
  with bare tickets still needs discovery for the gossip mesh
  to populate. The `pile net pull` path is fully address-
  threaded and works without discovery. Address-cache seeding
  for sync's gossip bootstrap is a follow-up (would require an
  `AddressLookup` provider plugged into iroh's
  `address_lookup` builder, or an `ep.connect()` seed pass
  at startup).

- `iroh-tickets 0.5` added as a dependency of both
  `triblespace-net` (for the rich-ticket print) and `trible`
  (for parsing). Pairs cleanly with iroh-base 0.98.

## [0.41.0] - 2026-05-16

The iroh-0.98 release. Replaces the 0.40.3 Cargo.lock workaround
for the upstream ed25519-dalek mess with a proper resolution.

### Changed
- **`triblespace-net` upgraded to the iroh 0.98 family.**
  - `iroh` 0.97 → 0.98 (still with `platform-verifier`)
  - `iroh-base` 0.97 → 0.98
  - `iroh-gossip` 0.97 → 0.98
  - `iroh-blobs` 0.99 → 0.100
  - `irpc` 0.13 → 0.14, `irpc-iroh` 0.13 → 0.14 (lock-step
    iroh-family bump)

  Upstream had pinned `ed25519-dalek = "=3.0.0-pre.1"` in
  `iroh-base 0.97`, which stopped compiling against
  `ed25519 v3.0.0` (released 2026-05-03) because
  `pkcs8::Error::KeyMalformed` changed from a unit variant to
  a tuple variant. `iroh-base 0.98` re-pins to
  `=3.0.0-pre.6`, which is API-compatible with current
  `ed25519`. Fresh `cargo install trible --locked` now
  resolves cleanly without needing the lockfile-shipping
  workaround that 0.40.3 used as a stopgap.

  No surface API changes in `triblespace-net` itself —
  iroh's `Endpoint::builder`, `presets::N0`,
  `CaRootsConfig::system()`, and the `ProtocolHandler`
  trait all kept their shape across 0.97 → 0.98. All 17 lib
  tests + 2 + 3 integration + 1 doctest pass.

- **Lock-step 0.40.x → 0.41.0 across all 8 workspace
  crates.** No source changes to `triblespace-core`,
  `triblespace-search`, `triblespace-macros{,-common}`,
  `triblespace-core-macros`, or the `triblespace` facade;
  versions bump to keep workspace alignment.

### Notes
- `trible 0.40.2` is yanked. `trible 0.40.3` (the Cargo.lock
  fix from earlier today) is left in place; it works but is
  obsoleted by 0.41.0. Downstream users on caret-permissive
  pins (`trible = "0.40"` will fall through to 0.40.3;
  `trible = "0.41"` picks up the proper fix).

## [0.40.2] - 2026-05-16

The TLS-roots-from-OS-store release. Patches one specific
failure mode in corporate-proxy / sandbox environments where
egress does TLS interception with a non-Mozilla CA.

### Fixed

- **`triblespace-net` now reads TLS trust anchors from the OS
  trust store** (via `rustls-platform-verifier`) instead of
  the compiled-in Mozilla `webpki-roots` bundle. The
  `platform-verifier` feature on iroh is enabled and the
  endpoint builder calls `.ca_roots_config(CaRootsConfig::system())`.

  Without this fix, sandbox environments that present a custom
  CA at TLS egress (e.g. Anthropic's web-sandbox's
  "sandbox-egress-production TLS Inspection CA") silently
  break iroh's discovery layer: every relay HTTPS probe and
  every pkarr publish/lookup to `dns.iroh.link` returns
  `invalid peer certificate: UnknownIssuer`, hole-punching
  never starts, and the QUIC peer handshake has no chance.

  Normal environments are unaffected — the OS trust store
  contains the same Mozilla roots that `webpki-roots` ships,
  so iroh's HTTPS to public infrastructure still works on
  macOS (Security framework), Linux (`/etc/ssl/certs`), and
  Windows (certificate store).

  Diagnosed by another Claude instance running in the web
  sandbox after the 0.40.0 tracing-instrumentation pass
  surfaced the `UnknownIssuer` WARN lines from iroh's
  internal logging. See `triblespace-net/CHANGELOG.md`.

- **`triblespace-core`, `triblespace-search`,
  `triblespace-macros{,-common}`, `triblespace-core-macros`**:
  lock-step 0.40.1 → 0.40.2 patch bump, no source changes.

## [0.40.1] - 2026-05-16

### Changed

- **`parallel` is now a default feature.** The workspace `triblespace` crate
  and `triblespace-core` enable it out of the box, so consumers get rayon
  transparently — no `--features parallel` needed to pick up the parallel
  query iterators and the `TribleSet::union` fan-out. WASM / embedded
  callers can still opt out via `--no-default-features`.

### Added

- **`TribleSet::union` 6-way rayon fan-out** (when `parallel` is on, which
  is now the default). The six trible indexes (`eav`/`eva`/`aev`/`ave`/
  `vea`/`vae`) touch disjoint memory during a union, so the per-index
  unions parallelise via nested `rayon::join` once `other.len()` clears
  `PARALLEL_UNION_THRESHOLD` (4096 tribles). Wins on the parallel
  `entities` bench family:

  | bench                       | 0.40.0   | 0.40.1   | delta   |
  |-----------------------------|----------|----------|---------|
  | union_parallel/5M           |  2.44 s  |  1.79 s  | -26.5%  |
  | union_parallel_chunked/2    |  224 ms  |  113 ms  | -49.5%  |
  | union_parallel_chunked/10   |  583 ms  |  247 ms  | -57.7%  |
  | union_parallel_chunked/100  |  1.75 s  |  794 ms  | -54.6%  |
  | union_parallel_chunked/1000 |  3.03 s  |  1.35 s  | -55.4%  |

  Serial fold (`union/5M`) sees ~5% feature-dispatch overhead because the
  per-`+=` `other` is too small to clear the threshold; small unions stay
  on the serial path.

## [0.40.0] - 2026-05-16

### Attribute id cache (perf)

`Attribute::id()` now reads from a cached `Id` field on `Attribute<S>`
instead of walking the wrapped Fragment's exports PATCH on every
call. The `From<Fragment>` impl captures the root id once at
construction. `entity!{}` codegen calls `.id()` once per attribute
per fact, so the pre-cache cost dominated the entities/union
benches:

| bench                     | size | pre-0.40 | 0.40   | delta vs pre |
|---------------------------|------|----------|--------|--------------|
| `entities`                | 5    | 2.88 µs  | 2.36 µs | recovered    |
| `union/5M`                |      | 9.00 s   | 7.94 s  | recovered    |
| `union_parallel/5M`       |      | 8.38 s   | 2.44 s  | recovered    |
| `union_prealloc/5M`       |      | 6.15 s   | 5.55 s  | recovered    |

(post-0.40 vs pre-conversion-arc baseline; full regression details
in commit `666e4764`.)

### Fragment annotation API simplification

- **`Fragment::annotated` and `Fragment::try_annotated` removed.**
  Saved 2-3 lines per call site at the cost of a closure
  indirection that obscured what was happening. The replacement
  pattern is `parent += entity!{ &id @ ... }` — when the
  annotation shares the parent's root (the common case),
  `Fragment += Fragment` re-unions the same id idempotently and
  folds facts + auto-put blobs through.
- **Schema describe collapse.** Every built-in `MetaDescribe::describe()`
  impl now reduces to a single `entity!{ ExclusiveId::force_ref(&id) @
  metadata::name: "...", metadata::description: "...", metadata::tag: ... }`
  expression. Auto-put through `entity!{}`'s blob-source machinery
  handles the string blobs; no more `Fragment::rooted + put + put +
  tribles += entity!{...}` dance. Net deletion of ~600 lines across
  the schema crate.
- **`Spread for Fragment` is allocation-free.** Replaced the
  `Vec<Id>` collect with `iter_ordered().map(raw_to_id)` using a
  free function pointer (so `Map`'s type is nameable in
  `Spread::Iter`). One fewer allocation per
  `Fragment::spread()` invocation.

### Conversion-system rewrite

A multi-step refactor of the value/blob conversion machinery
landed across 2026-05-14 → 2026-05-16. The user-facing surface is
now consistent under a single `Inline`/`Encoded`/`Encoding`/
`Encodes` vocabulary. On-disk format is unchanged — every
constant and metadata-attribute identifier that moved kept its
hex id.

#### Storage form: `Value<S>` → `Inline<S>`

The 32-byte stored payload is now `Inline<S>`. The name `Value`
is gone; `Encoded<V>` (below) is the higher-level sum that takes
its place.

- `Value<S>` (the 32-byte struct) → `Inline<S>`
- `RawValue` → `RawInline`
- `VALUE_LEN` → `INLINE_LEN`
- `UnknownValue` → `UnknownInline`
- `ValueRange` → `InlineRange`
- Method renames: `to_value` → `to_inline`, `from_value` →
  `from_inline`, `value_from` → `inline_from`,
  `try_to_value` → `try_to_inline`, `try_from_value` →
  `try_from_inline`.

#### Sum: `(Inline, Option<Blob>)` → `Encoded<V>`

The macro pipeline previously returned an `(Inline<V>,
Option<Blob<UnknownBlob>>)` tuple whose `Option` was `Some` iff
`V` was a `Handle` schema — an implicit invariant. Replaced with
a sum:

```rust
pub enum Encoded<V: InlineEncoding> {
    Inline(Inline<V>),
    Blob(Blob<UnknownBlob>),
}
```

`Encoded::inline()` rederives the typed handle from the blob's
cached Blake3 (phantom recast, no rehash). `into_parts()` yields
the old tuple for the macro consumer in one call. Initially named
`Value<V>`, renamed to `Encoded<V>` for vocabulary coherence.

#### Conversion: From-direction with blanket-derived ergonomics

Conversion is implemented schema-side (mirroring std's `From<T>`)
and source-side ergonomic methods are auto-derived:

```rust
pub trait Encodes<Source> {
    type Output;
    fn encode(source: Source) -> Self::Output;
}

pub trait IntoEncoded<S> {
    type Output;
    fn into_encoded(self) -> Self::Output;
}
impl<S, T> IntoEncoded<S> for T where S: Encodes<T> { ... }
```

Downstream impls no longer require "local type at trait position 0"
juggling — the schema sits at the impl-target, satisfying Rust's
orphan rule trivially.

- `ToValue` (old) → `IntoInline` (supertrait alias over `IntoEncoded`)
- `ToBlob` → `IntoBlob` (supertrait alias)
- `IntoValue` (interim) → `IntoInline`
- `IntoSchema` → `IntoEncoded`
- `into_schema` → `into_encoded`
- `IntoSchema::Form` → `IntoEncoded::Output`
- `FieldFormFor<V>` → `ToEncoded<V>` (the sum-lift dispatch shim)
- `ToValue` (the dispatch shim trait) → `ToEncoded`
- `to_value` (the dispatch shim method) → `to_encoded`
- `Attribute::into_field_value(v)` → `Attribute::encoded_from(v) ->
  Encoded<S>`, parallel to `Attribute::inline_from(v) -> Inline<S>`.

#### Trait family: `Schema` → `Encoding`

After removing semantic-marking schemas (Schema removals below)
the trait family genuinely describes encodings — byte format plus
validity plus identity. The name follows the role.

- `ValueSchema` → `InlineSchema` → `InlineEncoding`
- `BlobSchema` → `BlobEncoding`
- `InlineSchema::FieldKind` → `InlineEncoding::Encoding` (dispatch
  projection)
- Module renames:
  - `crate::value::*` → `crate::inline::*`
  - `value::schemas/` directory → `inline/encodings/`
  - `blob::schemas/` directory → `blob/encodings/`
  - `prelude::valueschemas` → `prelude::inlineencodings`
  - `prelude::blobschemas` → `prelude::blobencodings`
- Constants (Rust identifiers; hex ids unchanged):
  - `KIND_VALUE_SCHEMA` → `KIND_INLINE_ENCODING`
  - `KIND_BLOB_SCHEMA` → `KIND_BLOB_ENCODING`
- Attribute identifiers (Rust names; hex ids unchanged):
  - `metadata::value_schema` → `metadata::value_encoding`
  - `metadata::blob_schema` → `metadata::blob_encoding`

#### Schema removals

Two encodings whose distinction was *semantic* rather than
*structural* were removed. Semantic distinctions belong at the
attribute level, not the encoding level:

- `IRI` removed. Encoding is byte-identical to `LongString`; the
  semantic "this is an IRI" lives at the attribute. Removing it
  unlocks query unification (`Variable<Handle<IRI>>` and
  `Variable<Handle<LongString>>` couldn't unify before despite
  representing identical bytes) and ingestion robustness
  (validation at encoding boundary rejected mistyped IRIs;
  validation now lives at application boundary).
- `FileBytes` collapsed into `RawBytes`. Same decode target
  (`Bytes`), same validity (none); two ids labeling identical
  behavior. The "file-provenance" semantic lives at the attribute
  level.

`WasmCode` is kept distinct — its decode target is `WasmModule`
(structured type with its own validation), not just `Bytes`. The
schema label genuinely gates "safe to attempt WASM decode" and
prevents structural-but-garbage decodes (e.g. a PNG handle
decoded as `WasmModule`).

#### Eager handle caching (perf)

`Blob<S>` now caches its Blake3 handle at construction. This
eliminates a double-hash that surfaced at every
`MemoryBlobStore::insert` site in the `entity!{}` pipeline.
`Blob::with_handle` is the explicit "trust me" constructor for
read paths where the handle is already known. See commit
`536c364d`.

#### `entity!{}` auto-puts `Blob<T>` for `Handle<T>` fields

Passing a `Blob<T>` (or any blob-source like `&str`) as the value
for a `Handle<T>`-typed field absorbs the bytes into the
fragment's local blob store and uses the derived handle as the
trible's value. Replaces the explicit `ws.put(blob)` + handle
dance for the common case. See commit `8b8e7c0a`.

#### Items intentionally NOT renamed

- `value_range`, `value_in_range`, `entity_in_range`,
  `attribute_in_range` (query helpers; "value" refers to the V
  slot in (E, A, V) tribles, the slot name).
- `metadata::value_encoding` / `metadata::blob_encoding`
  (attribute identifiers; "value_" / "blob_" are part of the
  attribute name).
- `WasmValueFormatter`, `value_formatter` module/attribute (the
  "Value" here is generic "rendered value", not our Rust type).
- 3rd-party `Value`-named items: `clap::ValueEnum`,
  `proptest::strategy::ValueTree`, `Strategy::Value`,
  `serde_json::Value`.

#### Documentation

- `book/src/schemas.md` renamed to `book/src/encodings.md` with
  chapter title + intro rewrite.
- Doc-comment and prose updates across ~80 files to use the
  current Encoding vocabulary.
- README quickstart now demonstrates the `entity!{ note: "hi" }`
  auto-put pattern instead of the explicit `ws.put(...)` form for
  the canonical case.

## [0.39.0] - 2026-05-13

The canonical-attribute-id + origin-typed-identity + metadata-trait
unification release. Four related cleanups:

1. **Dynamic-name attribute id derivation** now goes through the
   same `entity!{...}.root()` mechanism every other entity uses,
   rather than bespoke flat-Blake3 hashing. The metadata
   `describe()` output and the attribute's identity come from a
   single source of truth.
2. **Each origin gets its own identity-determining attribute.** RDF
   predicates derive from `metadata::iri` (IRI is the canonical
   identifier); JSON fields and similar display-name-as-identity
   origins keep `metadata::name`. Collision is avoided structurally
   — an IRI-derived attribute and a same-bytes JSON-field-derived
   attribute differ in the (attr_id, value) pair feeding the
   intrinsic-id hash.
3. **`ConstId` + `ConstDescribe` collapsed into `MetaDescribe`** (renamed
   from interim `TypeDescribe`). The schema id is now
   `describe().root()` — one trait, one method, no separate identity
   trait. Every schema's identity-determining hex literal lives inline
   in its `MetaDescribe::describe` body. `const_blake3` (which existed
   to derive `Handle<H,T>::ID` and `Array<T>::ID` at compile time from
   `H::ID` / `T::ID`) is no longer needed: those types now derive their
   ids at runtime via the *entity-core* pattern (no-`@` `entity!` over
   a minimal identity-determining fact set; the fragment's intrinsic
   root IS the schema id). See wiki:c14041b4e1996a4101a1e80a8bdaa4c4
   ("Entity Core") for the mental model.
4. **`Fragment` is now self-contained.** It carries an internal
   `MemoryBlobStore<Blake3>` alongside its exports and facts, so any
   handle that appears in a fragment's facts has its bytes available
   without consulting an external blob store. `MetaDescribe::describe`
   collapses from `fn describe<B>(blobs: &mut B) -> Result<Fragment,
   B::PutError> where B: BlobStore<Blake3>` to plain `fn describe() ->
   Fragment` — the bytes live with the fragment that references them,
   describe is a pure (id, type) → Fragment function with no
   parameter threading and no error propagation, and `Describe for
   Attribute<S>` simplifies to `self.fragment.clone()`. See "Fragment
   self-containment" below for the full breakdown.

### Added
- **`blob::encodings::iri::IRI` BlobEncoding** for Internationalized
  Resource Identifiers. Byte layout matches `LongString` but the
  distinct schema lets handles carry their IRI-ness at the type
  level, enables boundary validation (`iri::looks_like_iri` —
  permissive RFC 3987 subset; debug-asserted at `ToBlob`), and makes
  IRI-derived attribute ids distinct from same-bytes
  LongString-derived ones. Re-exported as `prelude::blobencodings::IRI`.
- **`metadata::iri: Handle<Blake3, IRI>`** attribute. The canonical
  identity-determining attribute for RDF-imported entities.
  Distinct from `metadata::name` (which stays display-only).
- **`impl<S: InlineEncoding> From<Fragment> for Attribute<S>`** — the
  canonical dynamic-attribute constructor. Hand it an
  `entity!{ metadata::<identity-attr>: <value>,
  metadata::value_encoding: S::id() }` fragment whose root captures the
  identity-determining facts, and you get the typed attribute back.
  This is the *only* dynamic-attribute path: there is no specialized
  helper privileging any specific identity-attribute, so call sites
  must spell out which origin the id derives from (`metadata::name`
  for display-name origins, `metadata::iri` for RDF predicates, or
  whatever custom origin makes sense).
- **`metadata::array_item_schema: GenId`** attribute (id
  `56C43BEE48BE99521886D99BE9026A3B`). `Array<T>` references its
  element schema through this attribute rather than abusing
  `metadata::blob_encoding` (element schemas are not themselves
  `BlobEncoding`s).

### Changed (breaking)
- **`Attribute<S>` now stores a rooted `Fragment` (not just a raw
  id).** The wrapped fragment carries the identity-determining facts
  (`metadata::iri | metadata::name` + `metadata::value_encoding`),
  which `describe()` re-emits so the metadata registry stays
  queryable by IRI / name — that round-trip was lost in the prior
  `raw: RawId`-only shape. `id()` becomes
  `self.fragment.root().expect("rooted")`.
- **`Attribute::<S>::from_name`, `from_iri`, `from_id`, and
  `from_id_with_usage` removed.** The single public construction
  path is `impl<S: InlineEncoding> From<Fragment> for Attribute<S>`.
  Replace each call with explicit `Attribute::<S>::from(entity!{ … })`,
  naming the identity attribute (`metadata::name`, `metadata::iri`,
  or an explicit `@`-prefixed hex id) at the call site:
  ```rust
  // display-name origins (JSON fields, config keys, column headers):
  Attribute::<S>::from(entity! {
      metadata::name:         name.to_blob().get_handle::<Blake3>(),
      metadata::value_encoding: <S as MetaDescribe>::id(),
  })

  // RDF / JSON-LD predicates (IRI as canonical identifier):
  Attribute::<S>::from(entity! {
      metadata::iri:          iri.to_blob().get_handle::<Blake3>(),
      metadata::value_encoding: <S as MetaDescribe>::id(),
  })

  // Explicit hex id (schema pinning, bootstrap attrs):
  let id: Id = id_hex!("…");
  Attribute::<S>::from(entity! { &ExclusiveId::force_ref(&id) @
      metadata::value_encoding: <S as MetaDescribe>::id(),
  })
  ```
  The derivation is unchanged — canonical
  sorted+deduped+Blake3-hashed (attr, value) pairs, lo16 bytes — so
  attribute ids for migrated callers stay the same; only the call
  shape changes.
- **`attributes!{ "hex" as name: schema; … }`** no longer produces
  `const Attribute<S>` — Fragment isn't const-constructible, so
  fixed-id attrs become `static LazyLock<Attribute<S>>` like
  derived ones. Within the LazyLock init, the Hex branch
  constructs via `Fragment::rooted(id, TribleSet::new())` (low-
  level API, no `entity!{}`) to avoid a bootstrap deadlock —
  foundational attributes like `metadata::value_encoding` would
  otherwise reference themselves during their own init.
- **`Describe for Attribute<S>`** is a pure accessor: it returns
  `self.fragment.clone()` and nothing else. The wrapped identity
  fragment already carries `metadata::iri` / `metadata::name`
  together with `metadata::value_encoding: S::id()` from construction,
  which is the complete identity-determining fact set. Schema-level
  facts (the schema's own name, description, hash protocol info)
  belong to the schema, not the attribute — consumers wanting them
  call `<S as MetaDescribe>::describe()` directly. Drops the
  `S: MetaDescribe` bound on the impl (no longer needed); no `blobs`
  parameter is threaded through (no blob puts needed to describe an
  attribute, and `describe()` is infallible). Per-attribute
  `describe()` also doesn't emit usage facts — those live in the
  macro-generated top-level `describe()` as separate usage entities.
- **`AttributeUsage` / `AttributeUsageSource` types removed.**
  An `attributes!{}` declaration site IS an attribute usage; the
  abstract attribute is the shared thing multiple parties agree
  on, and the macro emits the codebase-local annotations (rust
  identifier as `metadata::name`, `module_path!()` as
  `metadata::source_module`, doc comment as
  `metadata::description`) inline at the declaration site, in the
  macro-generated top-level `pub fn describe() -> Fragment` function.
  Per-attribute `Attribute<S>` no longer carries usage data, and
  there is no `with_usage` builder. The usage entity's id and
  fact structure are byte-identical to the prior
  `AttributeUsage::describe` output (`(metadata::attribute,
  metadata::source_module)` → usage id; `metadata::name`,
  `metadata::tag: KIND_ATTRIBUTE_USAGE`, optional
  `metadata::description` under the usage id).
- **`Fragment::annotated` added.** Collapses the recurring three-step
  pattern:
  ```rust
  let mut frag = entity! { <core facts> };
  let id = frag.root().expect("rooted");
  frag += entity! { &ExclusiveId::force_ref(&id) @ <annotations> }.into_facts();
  ```
  into a single chained call:
  ```rust
  entity! { <core facts> }.annotated(|id_ref| {
      entity! { id_ref @ <annotations> }
  })
  ```
  The annotation fragment's facts merge in but its root is dropped —
  `self.root()` still returns the pre-annotation id. With `describe()`
  no longer fallible the previously-paired `try_annotated` variant is
  gone; closures that need to add blobs to the fragment now do so via
  the *outer* `Fragment::put` before calling `annotated`. Used by
  `Describe for Attribute<S>` (schema spread) and by the
  `attributes!{}` macro's per-attribute usage emission, so the
  generated code no longer has the temp-root extraction dance.
- **`attributes_impl` no longer invokes a sibling proc-macro for
  `entity!{}` expansions**. It calls `entity_impl` (same crate)
  directly, expanding to a `TokenStream2` with the
  `attributes_impl` caller's own `base_path`. The two macro shims
  (`triblespace_core_macros::attributes` →
  `::triblespace_core` paths; `triblespace_macros::attributes` →
  `::triblespace::core` paths) keep working as before, but
  attribute declarations no longer emit *N* inner `emit_metadata`
  invocation records per `attributes!{}` block — only the outer
  user-facing macro invocation gets recorded by the metadata
  emitter.
- **`ImportAttribute` removed.** It was a thin wrapper around two
  separate patterns: (1) "build an attribute from a name handle"
  (now just `Attribute::<S>::from(entity!{ metadata::name: handle,
  metadata::value_encoding: <S as MetaDescribe>::id() })` in the
  JSON object importer) and (2) "attach a contextual name fact to
  an existing attribute id" (the `import::json_tree::build_json_tree_metadata`
  rename pattern, which is gone — the macro-generated `describe()`
  already emits a usage entity with `metadata::source_module:
  "triblespace_core::import::json_tree"`, which disambiguates the
  module's view of each attribute without needing a separate
  `json.kind` / `json.string` / … rename. Nothing in the codebase
  queried those rename strings.)

  **Tooling-side migration**: any external metadata-browser that
  previously string-matched `?attr @ metadata::name = "json.kind"`
  directly on attribute entities will not find that fact in fresh
  manifests. The new shape is a usage entity:
  `?usage @ metadata::attribute = <attr_id>,
            metadata::source_module = <handle of "triblespace_core::import::json_tree">,
            metadata::name = "kind"`.
  Old piles still contain the direct-name facts and remain readable;
  mixed old/new manifests will surface both shapes, so name-discovery
  tooling should fall back to the usage-entity query if the direct
  one yields nothing.
- **`import::ntriples`** now derives all predicate URI attributes
  through `metadata::iri` (the `NTriplesAttrCache` builds the
  per-(IRI, S) `Attribute` via the inlined entity-core pattern).
  Net effect: RDF-imported attribute ids change to new values that
  ALSO differ from JSON field name-derived ids on the same byte
  content.
- **`ConstId` trait removed.** Every schema's identity-determining
  hex literal moves from `impl ConstId for X { const ID: Id =
  id_hex!("…"); }` to an inline `let id: Id = id_hex!("…");` inside
  its `MetaDescribe::describe` body. Callers reach the id via
  `T::id()` (default = `T::describe().root()`).
- **`ConstDescribe` renamed to `MetaDescribe`.** The trait emits
  facts in the `metadata::*` namespace; the new name signals the
  intent rather than the call shape. Mechanical rename — same method
  signature, same default `id()` derivation.
- **`HashProtocol` super-trait now `+ MetaDescribe`** (was `+
  ConstDescribe + ConstId`). The id flows through describe like
  every other schema; the bound stops conflating "I have a stable
  identifier" with "I implement a digest function".
- **`InlineEncoding` and `BlobEncoding` super-traits now `+ MetaDescribe`**
  (was `+ ConstId`). Schemas must describe themselves; the id is a
  property of that description, not a separate trait method.
- **`Handle<H,T>::describe`, `Array<T>::describe`, and
  `Attribute<S>::describe` use the entity-core split with `entity!`'s
  `*:` spread syntax** — sub-schemas are described *once* and their
  roots become the values of `metadata::blob_encoding` /
  `metadata::hash_schema` / `metadata::array_item_schema` /
  `metadata::value_encoding`, while their facts fold into the parent
  fragment automatically. Annotations (name, description, tag) attach
  via `&id @ …` so reworking documentation doesn't rotate the id.
  Net effect: `Handle<Blake3, LongString>::id()` and similar
  derived-id schemas have *new* id values vs. 0.38.0's `const_blake3`
  hashes. Re-ingest is required (consistent with the 0.39 attribute-
  id break above).
- **`Array<T>` uses `metadata::array_item_schema` (not
  `metadata::blob_encoding`)** to reference its element type. Element
  schemas (`array::F32`, `array::U8`, …) are not themselves
  `BlobEncoding`s — they only carry an `ArrayElement::Native` byte
  layout — so the dedicated attribute prevents semantically misleading
  edges. The id derivation is structurally the same shape but
  attribute-id differs, so existing `Array<T>` ids rotate again.
- **`const_blake3` workspace crate dropped.** Was a `triblespace-core`
  dep purely for compile-time `Handle`/`Array` id derivation;
  superseded by the runtime entity-core path. Workspace member,
  path dependency, and the `const-blake3/` directory are all gone.
- **Blanket `impl<T: ConstDescribe> Describe for T` dropped.**
  Instance `Describe` and type-level `MetaDescribe` are now distinct
  concepts; calling `Boolean.describe()` (instance-method form on a
  unit-struct schema marker) no longer compiles — use
  `Boolean::describe()` (associated-fn form) instead. No in-repo
  callers used the blanket; the change is documented for downstream
  crates.
- **`MetaDescribe::id()` is runtime, not const.** Pre-`0.39.0` code
  could use `T::ID` in `const` contexts. Post-rename `T::id()` is a
  fn that runs `T::describe().root()` each call.
  `Attribute<S>` amortizes per attribute via its stored
  `fragment.root()` lookup (cheap — single PATCH read). Hot
  dispatch sites that call `<S as MetaDescribe>::id()` repeatedly
  should hoist via `LazyLock<Id>` — see
  `triblespace-core/src/export/json.rs::render_schema_value`.

### Fragment self-containment
- **`Fragment` carries an internal `MemoryBlobStore<Blake3>`**
  alongside its exports and facts. The shape goes from
  `{ exports: PATCH<16>, facts: TribleSet }` to
  `{ exports: PATCH<16>, facts: TribleSet, blobs: MemoryBlobStore<Blake3> }`.
  Any handle that appears in the fragment's facts has its bytes
  available *with* the fragment — no external store lookup needed.
  An empty `MemoryBlobStore` is structurally a single PATCH-root
  pointer, so fragments without blobs pay essentially zero
  overhead.
- **New `Fragment` API:**
  - `put<S, T>(&mut self, item: T) -> Inline<Handle<Blake3, S>>` —
    insert a blob into the fragment's local store and get the
    content-addressed handle back. Idempotent.
  - `blobs() -> &MemoryBlobStore<Blake3>` — read the embedded
    store.
  - `into_facts_and_blobs(self) -> (TribleSet, MemoryBlobStore<Blake3>)` —
    consume the fragment, drop the exports, keep the payload.
  - `from_facts_and_blobs`, `rooted_with_blobs`, three-tuple
    `into_parts` for low-level wrap/unwrap.
  - `Fragment += Fragment` (`AddAssign`) now also unions the
    embedded blob stores. `TribleSet += Fragment` still drops
    blobs (facts-only merge); pull blobs out with
    `into_facts_and_blobs` if you need them downstream.
- **`Spread::spread` returns `(Iter, TribleSet, MemoryBlobStore<Blake3>)`**
  instead of `(Iter, TribleSet)`. The `entity!{ field*: spread_source }`
  syntax now propagates blobs from spread sources into the parent
  fragment automatically — a spread of a sub-schema's `describe()`
  fragment carries that schema's documentation blobs forward without
  any caller-side bookkeeping.
- **`MetaDescribe::describe` signature collapses from**
  ```rust
  fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
  where B: BlobStore<Blake3>;
  ```
  **to**
  ```rust
  fn describe() -> Fragment;
  ```
  No `<B>` parameter, no `Result`, no `?` threading just to bubble
  `B::PutError`. Schemas build their fragments via
  `Fragment::put(item)` on a local
  `Fragment::rooted(id, TribleSet::new())` and then fold
  annotations via `Fragment::annotated`. The bytes live with the
  fragment that references them.
- **`Describe::describe(&self) -> Fragment`** likewise drops `<B>` /
  `Result`. The instance form is now a pure (self → Fragment)
  accessor. `Describe for Attribute<S>` shrinks to a one-liner:
  `fn describe(&self) -> Fragment { self.fragment.clone() }`.
- **`MetaDescribe::id()` default** is `Self::describe().root().expect(…)`
  (no scratch store needed). `Describe::id(&self)` parallels.
- **`try_annotated` removed.** With describe infallible the
  fallible annotation variant has nothing to propagate; closures
  that need to add blobs to the surrounding fragment do so via
  `Fragment::put` on the *outer* fragment before calling
  `annotated`. The `annotated` debug-assertion that the closure's
  returned fragment is rooted at the same id remains.
- **`MemoryBlobStore<H>` gains `Clone`, `PartialEq`, `Eq`** so
  Fragment can derive the same traits cleanly.
- **JSON importers' `metadata()` is infallible.** `JsonObjectImporter::metadata()`
  and `JsonTreeImporter::metadata()` both return `Fragment` directly.
  `build_json_tree_metadata` drops its blob-store parameter; it
  builds a self-contained fragment internally and returns it.
- **`entity!{}` macro emits a `MemoryBlobStore<Blake3>` accumulator**
  alongside the `TribleSet`, unions spread-source blobs into it, and
  wraps the final result via `Fragment::rooted_with_blobs`. Pure
  `entity!{}` calls with no spreads compile to an empty store
  (single PATCH pointer) — no overhead vs. the old `Fragment::rooted`
  shape.
- **Conceptual win.** `describe()` is now a pure function from a
  type/instance to a self-contained Fragment. No state mutation,
  no fallibility, no parameter threading. The "where do the bytes
  live" question collapses to a single answer: with the Fragment
  that references them.

### Migration
- **Attributes declared with explicit hex via `attributes! { "ID"
  as name: schema; ... }`** are unaffected. Their ids are stable.
- **Attributes derived from URIs/field-names** (the RDF and JSON
  importers' default path; `wd_bench::loader::predicate_id` for
  cookbook recipes) get new ids. Re-ingest the source data into a
  fresh pile to pick them up. No automatic migration of existing
  piles — we caught this design issue before the system has any
  external users, so the cleanest move is to break and re-ingest.

### Why this matters
- One canonical content-addressing mechanism for entity ids. The
  `describe()` metadata that documents an attribute now *is* the
  attribute's identity — adding a new dimension (cardinality,
  inverse-of, etc.) to the description automatically participates
  in the id derivation, with no hand-rolled hash to update.
- Sets up the eventual "URI position-asymmetry" cleanup: predicate
  attributes and rdf-position entities will both go through
  `entity!` so the asymmetry becomes a query distinction (which
  attribute facts describe the URI's role) rather than a hash-
  formula distinction.
- Every dynamic-id-deriving path in core now goes through one
  mechanism. `blake3::Hasher` is no longer imported anywhere in
  `attribute.rs` / `import/import_attribute.rs` (the macro handles
  hashing internally).

### Path-query: bounded-depth closure estimation
- **`estimate_from`'s closure-fallback no longer full-materialises**
  the result set (`triblespace-core/src/query/regularpathconstraint.rs`).
  When shallow estimation doesn't apply — i.e. the path body
  contains an unbounded closure that can't be re-shaped through
  the WCO `build_join` — the previous fallback ran
  `eval_from(set, body, start).len()`, which paid the full cost
  of computing the closure just to measure its size. The new
  `bounded_eval_from` helper caps closure BFS at
  `RPQ_ESTIMATE_DEPTH = 5` levels, matching Karalis et al.
  ESWC 2024 §4.3's "default estimation" technique. Bounded depth
  → bounded estimate cost, sufficient for driving the WCO
  planner's variable ordering without paying for the materialisation
  it was meant to inform. Non-closure expressions (Attr,
  InverseAttr, Concat, Union) don't consume depth — the bound
  only fires on Plus/Star iteration steps.
- Shallow estimation (the constant-time per-attribute count from
  the segmented index) was already in place; this commit just
  closes the remaining gap where shallow doesn't apply. All 10
  path proptests pass; 49 broader query proptests pass.

## [0.38.0] - 2026-05-07

The team-rooted-gossip release. The gossip mesh id is now
derived directly from the team root pubkey, so `triblespace-net`
and `trible` no longer ask users to coordinate a separate topic
string with their team. One identifier per team handles both
auth (cap chain verification) and rendezvous (gossip mesh).

### Changed (breaking)
- **`triblespace::net::peer::PeerConfig.gossip_topic:
  Option<String>` → `gossip: bool`.** When `gossip = true`, the
  topic is `team_root.to_bytes()` directly (32 uniform bytes
  from the ed25519 pubkey — perfect as a `TopicId`, no hashing
  needed). `gossip = false` is serve/pull-only (no mesh
  subscription). Migration: `Some(_)` → `true`, `None` → `false`.
  See `triblespace-net/CHANGELOG.md`.
- **`trible pile net sync --topic NAME` flag removed.** Sync
  always joins the team's gossip mesh, identified by
  `TRIBLE_TEAM_ROOT` (or single-user fallback to the node's own
  pubkey when unset). Migration: drop the `--topic` flag from
  any sync invocation. See `trible/CHANGELOG.md`.
- All 8 workspace crates bumped 0.37.0 → 0.38.0 in lock-step
  (`triblespace`, `triblespace-core`, `triblespace-core-macros`,
  `triblespace-macros`, `triblespace-macros-common`,
  `triblespace-net`, `triblespace-search`, `trible`). Only
  `triblespace-net` and `trible` carry source changes; the rest
  bump to keep workspace versions aligned.

## [0.37.0] - 2026-05-06

The search release. `triblespace-search` (BM25 + HNSW indexes
on top of triblespace piles) lands as a first-class workspace
crate; the umbrella re-exports it at `triblespace::search`
behind the `search` feature. Includes the canonical-bytes
storage-layout refactor, the auth-arc test maturation, and a
range-query primitive in core.

### Added
- **`triblespace-search`** — first crates.io release of the
  BM25 + HNSW search crate. Two blob types
  (`SuccinctBM25Blob`, `SuccinctHNSWBlob`) backed by zero-copy
  `anybytes`-frozen `ByteArea` bytes; the index *is* its blob,
  so `ToBlob` is an `O(1)` refcounted clone. Re-exported at
  `triblespace::search` behind the `search` feature. Full
  surface details in `triblespace-search/CHANGELOG.md`.
- **`Universe::search_range` / `search_lower` / `search_upper`**
  in `triblespace-core` — log-time range primitives over a
  monotonic universe, with `O(log n + K)` `value_in_range`
  proposals on `SuccinctArchive`. See
  `triblespace-core/CHANGELOG.md`.
- **`trible team show [--verify]`** end-to-end chain walk +
  `verify_chain` rehearsal against a configured team root.
  Same code path the relay's `OP_AUTH` uses; result mirrors
  what a real connection attempt would see.
- **`trible team invite --branch <BRANCH_HEX>`** restricts the
  issued cap to the named branch(es); `team list` surfaces
  the per-cap details (issuer → subject, perms, branches,
  expiry — sorted soonest-first) and the `(revoker, target)`
  pairs for each verifiable revocation.
- **`pile net status`** diagnostic prints the node id, team
  root, and self_cap a peer would present on `OP_AUTH`,
  annotated with their source ("from `TRIBLE_TEAM_ROOT`",
  "single-user fallback", "NOT SET — remote will reject").
- **Live revocation propagation** in `triblespace-net`: each
  `Peer::refresh` rescans the new snapshot for `(rev, sig)`
  blob pairs signed by the configured team root and unions
  them into the live revoked set. No restart needed for a
  revocation gossiped into the pile.
- **Capability auth book chapter**
  (`book/src/capability-auth.md`) covering the model, CLI
  lifecycle, wire protocol, two-tier scope gate, and
  revocation. Linked from the workspace TOC.
- **`pattern!` free-attribute form** — `{ ?e @ ?attr: ?val }`
  patterns where the predicate slot is a query variable.
  Building block for SPB-style outer projections (Q3/Q4
  `?cw ?pred ?value`) and general-purpose schema-erased
  iteration over an entity's triples. The value variable
  must be typed `Variable<UnknownInline>` (compile-time
  enforced); decoding to a concrete schema is an explicit
  `try_from_inline::<S>()` step at the use site.
- **`PathOp::Optional` (`(p)?`) primitive** in the path-query
  language. Zero-or-one application of a sub-path; recognised
  inline so the zero-step branch reuses the bound start node.
  Plus a `from_postfix`-time normalisation pass that lifts
  `Optional`/`Union` out of `Concat` (`a / b? ↔ a | (a / b)`)
  so the typical WDBench shape `p / q?` actually translates.
  See `triblespace-core/CHANGELOG.md`.
- **`PathOp::Inverse` (`^p`) primitive** in the path-query
  language. Per-attribute reverse traversal via the VAE
  index; compound expressions normalise via standard reversal
  rewrites. See `triblespace-core/CHANGELOG.md`.

### Changed
- **Pile-sync protocol stays at v4** (`/triblespace/pile-sync/4`)
  but the test suite matures: e2e iroh-backed auth tests are
  un-ignored, three pass green over real `TestNetwork`
  endpoints (smoke + AUTH_OK + AUTH_REJECTED). Reachability
  BFS for `OP_CHILDREN` is amortised across responses.
- **`triblespace-search`'s `pub bytes: Bytes` is the persistence
  surface** — the canonical-bytes pattern (mirroring
  `SuccinctArchive`) means `ToBlob` is `Bytes::clone`,
  `to_bytes` / `try_from_bytes` retired. Schema ids rotated
  for both blob types — see search CHANGELOG.
- **`Cargo.lock` ethnum bumped to 1.5.3** (was 1.5.2). Fixes
  the transmute UB on nightly that was failing docs.rs builds
  for `triblespace 0.36.0`. Constraint stayed at `^1.5.0`, so
  this release picks up the fix automatically; the failing
  build will be replaced when 0.37.0 publishes.

### Removed
- Pre-existing rustdoc-link warnings across the workspace —
  14 `unresolved link` / `links to private item` warnings
  cleared in `triblespace-core` and `triblespace-search`.
  `cargo doc --workspace --no-deps` is now warning-free.

## [0.36.0] - 2026-04-28

The chain-of-trust capability auth release. New
`triblespace_core::repo::capability` module + protocol v4 + `trible
team` CLI surface. See `book/src/capability-auth.md` for the
user-facing chapter and the per-crate CHANGELOGs
(`triblespace-net/CHANGELOG.md`, `trible/CHANGELOG.md`) for the
surface-level details. Highlights:

### Added
- **`triblespace_core::repo::capability`** — chain-of-trust
  capability lib: `build_capability` / `verify_chain` /
  `build_revocation` / `extract_revocation_pairs` /
  `scope_subsumes`, plus the `VerifiedCapability` type with
  `permissions` / `granted_branches` / `grants_read` /
  `grants_read_on` helpers. 27 lib tests; runnable rustdoc
  examples on every primary public fn.
- **Pile-sync protocol v4** (`/triblespace/pile-sync/4`):
  mandatory `OP_AUTH` first stream, two-tier scope gate
  (branch level on `OP_LIST` / `OP_HEAD`, blob-reachability on
  `OP_GET_BLOB` / `OP_CHILDREN`), live revocation propagation
  through snapshot rescans.
- **`trible team {create, invite, revoke, list}`** subcommand group;
  `team invite --branch <BRANCH_HEX>` for branch-restricted caps;
  `team list` audits caps with issuer→subject, scope, expiry sorted
  soonest-first.

### Changed
- `triblespace::net::peer::PeerConfig` is now non-`Default` —
  every construction site must specify `team_root`, `revoked`,
  `self_cap`.
- `trible pile net sync` / `pile net pull` read `TRIBLE_TEAM_ROOT`
  + `TRIBLE_TEAM_CAP` env vars for multi-user team operation.

## [0.19.0] - 2026-03-13
### Changed
- **Breaking:** Renamed the `matches!` query macro to `exists!` to resolve the
  name collision with `std::matches!` that made the macro unusable in practice.

## [0.35.0] - 2026-04-18
### Breaking
- **`Id::aquire` → `Id::acquire`** (fixing a long-standing typo).
  Paired: `ExclusiveIdError::FailedAquire` → `FailedAcquire`.
- **Commit metadata is now content-addressed.** `commit_metadata`
  derives the commit's entity id intrinsically from its
  `(attribute, value)` pairs via `entity!`'s content-hash form instead
  of minting a random `rngid()`. Merge commits (content = `None`) also
  drop `metadata::created_at` since no authorial act produced them.
  Existing piles aren't invalidated — old commits with random entity
  ids remain queryable — but newly-minted commits have different
  entity ids and therefore different blob hashes than the pre-change
  world. Payoff: two peers merging the same parent set produce
  bit-identical merge commits, so parallel-merge scenarios in
  distributed sync converge in zero extra rounds via content
  addressing.
- **Branch metadata is now content-addressed the same way.**
  `branch_metadata` and `branch_unsigned` use `entity!`'s intrinsic id
  form instead of the deleted `derive_metadata_entity` helper. Every
  publish also stamps `metadata::updated_at: NsTAIInterval` so peers
  can order concurrent HEAD gossips without an ancestor walk.
  Tradeoff: because `updated_at` varies per publish, the same
  `(head, name, signer)` state at two different moments no longer
  produces an identical metadata blob hash.

### Added
- `SortedSlice::from_mut(&mut [T])`: sort-in-place constructor that
  mirrors the `new_unchecked` ergonomics when the caller has a mutable
  slice but no pre-sortedness guarantee.
- `ContainsConstraint` impl for `&'a mut [T]` that sorts the slice in
  place and produces a `SortedSliceConstraint`. Via `DerefMut` method
  resolution this also picks up `&mut Vec<T>`, `&mut [T; N]`,
  `&mut Box<[T]>`, and any other mutable borrow that derefs to a slice,
  so callers can write `(&mut my_vec).has(var)` without hand-rolling
  the sort.
- `import::ntriples::{ingest_ntriples, ingest_ntriples_file}`:
  N-Triples importer generic over any
  `Workspace<Blobs: BlobStore<Blake3>>`. XSD datatypes map to native
  triblespace schemas (`xsd:integer` → `I256BE`, `xsd:decimal` →
  `R256BE` exact rational, `xsd:float`/`xsd:double` → `F64`,
  `xsd:boolean` → `Boolean`, strings → `Handle<LongString>`, URI
  objects → `GenId`). Predicate URIs become attributes via
  `Attribute::from_name` so repeated imports of the same data converge
  deterministically.
- `import::rdf_uri`: canonical "this entity is the referent of this
  URI" attribute, used by the N-Triples importer to derive stable
  entity ids from URIs.
- `triblespace-net` joins the workspace as a first-class member. The
  facade crate gains a `net` feature
  (`triblespace = { version = "x", features = ["net"] }`) that
  re-exports it as `triblespace::net`, so
  `use triblespace::net::peer::Peer;` is the one-liner for distributed
  sync. The subtree merge preserves the full commit history from the
  previously-standalone repo.
- `triblespace-net` now inlines the `iroh-dht` implementation as an
  internal module (`triblespace_net::dht`) instead of depending on the
  separate `iroh-dht` crate. The implementation was a triblespace fork
  of `iroh-dht-experiment` with API migration and a `ContentDiscovery`
  trait impl; integrating it into triblespace-net eliminates the
  unpublished-fork blocker for the `net` feature and keeps DHT
  evolution co-located with its only consumer.
- **Edition bump**: `triblespace-net` is now on Rust edition 2024
  (inherited from iroh-dht's let-chain syntax). Users depending on
  `triblespace-net` directly or on the facade's `net` feature need
  Rust 1.85 or newer.

### Changed
- `Pile::put` now handles blobs larger than the kernel's atomic
  `write_vectored` ceiling (~2&nbsp;GiB on macOS / Linux). Records
  below a 1&nbsp;GiB threshold keep the existing shared-lock +
  single-`writev` fast path; larger records take an exclusive lock and
  append via plain `write_all`, lifting the previous ~2&nbsp;GiB
  per-blob cap. The exclusive-lock path remains append-only and
  `Pile::restore` still truncates any partial tail after a crash.
  Test coverage added as `put_and_get_oversized_blob`
  (`#[ignore]`d because the exercise allocates ~1&nbsp;GiB of memory).

### Documentation
- New book chapter: **"Distributed Sync"** (under Repositories &
  Workflows) covers the `Peer<S>` mental model, gossip / DHT / QUIC
  transports, `track` vs `fetch` primitives, `merge_tracking_into_local`,
  convergence rounds under sequential vs parallel gossip, and the CLI
  surface (`trible pile net {identity, sync, pull}`).
- "Importing Data Formats" chapter gains an "Importing N-Triples"
  section with the XSD → triblespace schema mapping table and a query
  roundtrip example.
- "Deep Dive: Identifiers" chapter reframed around clearer axes:
  *derivability* (intrinsic/extrinsic = "can the id be recomputed from
  the entity?") and *content encoding* (abstract/semantic = "do the
  bits carry meaning about the entity?"). New "Quadrant Properties"
  section names the structural invariants (extrinsic + semantic +
  global scope ⇒ authority; the other quadrants can be decentralized).
- `book.toml` enables MathJax so the chapters' `\( 2^{128} \)` notation
  actually renders.

## [0.34.1] - 2026-04-04
### Added
- Optional `telemetry` feature in the facade crate:
  - `triblespace::telemetry::Telemetry` for pile-backed tracing sinks
  - `triblespace::telemetry::TelemetryLayer` for embedding into custom
    subscribers
  - `triblespace::telemetry::schema` metadata/attribute ids used by the sink
  - environment controls: `TELEMETRY_PILE`,
    `TELEMETRY_FLUSH_MS`.

### Changed
- Trimmed `triblespace::telemetry` schema to generic span/session fields by
  removing GORBIE-specific `card_index` capture from the shared sink.
- `exists!` now supports the zero-variable form `exists!(constraint)` for pure
  existence checks without the tuple head ceremony.

### Fixed
- `PATCH::difference` now returns an empty set when the left-hand side is
  empty (`∅ \ B = ∅`) instead of incorrectly cloning the right-hand side.
- `find!` now rejects the common footgun where a projected variable never
  appears in the constraint tokens, and the fallback unbound-variable panic now
  points users toward `find!((), ...)` / `exists!(constraint)`.
- Pile-backed tests now create the pile file explicitly before calling
  `Pile::open`, matching the newer no-auto-create semantics and restoring the
  full workspace test suite.

### Documentation
- Documented `PushError::StoragePut` guidance for large local `Pile` writes:
  platform `writev` limits can surface `EINVAL`, and oversized payloads should
  be chunked semantically behind a manifest/root record.
- Added rustdoc coverage for the public macro surface and a new book chapter,
  "Macro Cookbook", with runnable doctest examples for the main query and data
  construction macros.

## [0.20.0] - 2026-03-14
### Changed
- **Breaking:** Removed the `FromInline` trait. `TryFromInline` is now the sole
  value conversion trait. `Inline::from_inline()` is constrained to
  `TryFromInline<Error = Infallible>`.
- **Breaking:** `find!` now uses filter semantics: when a variable's
  `TryFromInline` conversion fails the row is silently skipped instead of
  panicking. For types with `Error = Infallible` (e.g. `f64`, `Inline<_>`) no
  rows can ever be accidentally filtered.
- **Breaking:** `find!` variable declarations support a `?` suffix
  (`name: Type?`) that yields `Result<T, E>` without filtering, matching
  Rust's `?` semantics of "bubble the error to the caller."
- **Breaking:** `Query::new` now expects the post-processing closure to return
  `Option<R>`. Returning `None` skips the current binding and continues the
  search. Direct callers of `Query::new` must wrap their return values in
  `Some(...)`.
- `find!` is now implemented as a hybrid `macro_rules!` + proc macro
  (`__find_impl!`), replacing the previous three-arm `macro_rules!` definition.
- `HashSet`/`HashMap` constraint bounds relaxed from requiring
  `TryFromInline<Error = Infallible>` to accepting any `TryFromInline`; values
  that fail to convert are rejected during `confirm()`.

## [0.16.0] - 2026-02-15
### Changed
- JSON importer metadata builders now return `Fragment`:
  - `import::json::JsonObjectImporter::metadata`
  - `import::json_tree::JsonTreeImporter::metadata`
  - `import::json_tree::build_json_tree_metadata`
  These fragments export the derived schema/attribute/kind ids to make merging
  and discovery more convenient.

## [0.15.0] - 2026-02-15
### Changed
- Cleanup/polish after 0.14.0: fixed benches and minor clippy lints, and added
  small convenience helpers (`Universe::is_empty`).
- `metadata::ConstDescribe::describe` now returns a rooted `Fragment` (exporting
  the schema id) instead of a raw `TribleSet`, aligning constant schema metadata
  with runtime `Describe`.
### Added
- Additive set ergonomics: `TribleSet + Fragment` and `Fragment + TribleSet`
  (plus corresponding `+=` forms) to union facts while preserving exports on
  the fragment side.

## [0.14.0] - 2026-02-14
### Added
- `Fragment`: a rooted (or multi-root) graph fragment that bundles a `TribleSet`
  with exported entity id(s).
- `Id::raw()`: a `const` helper returning the raw 16-byte identifier.

### Changed
- `entity!` now returns a `Fragment` instead of a raw `TribleSet`. Merge facts
  into datasets via `+=` (only facts are unioned). Use `.into_facts()` when you
  explicitly need to drop exports and work with a plain `TribleSet`.
- `Workspace::commit` now accepts `impl Into<TribleSet>` for content, so you can
  commit a `Fragment` directly.
- Renamed metadata traits: `metadata::Metadata` -> `metadata::Describe` and
  `metadata::ConstMetadata` -> `metadata::ConstDescribe`.
- `metadata::Describe::describe` now returns a `Fragment` (with exports as the
  described root id(s)) and no longer has a separate `id()` accessor.
- Introduced `metadata::ConstId` (`const ID: Id`) for schema identifiers and
  kept `metadata::ConstDescribe` focused on emitting optional discovery
  metadata. Composite `Handle` schema IDs are derived in `const` context via
  the new `const_blake3` workspace crate.
- `import::json::JsonObjectImporter::import_*` now returns a `Fragment` instead
  of root id lists and no longer retains accumulated facts internally (removed
  `data()`/`clear_data()`; callers merge fragments explicitly).
- `import::json_tree::JsonTreeImporter::import_*` now returns a rooted `Fragment`
  and no longer retains accumulated facts internally (removed `data()`).

## [0.13.2] - 2026-02-13
### Added
- `entity!` now supports repeated facts via `attr*: iter_expr`.

## [0.13.1] - 2026-02-13
### Added
- `entity!` now supports optional facts via `attr?: option_expr`.

## [0.13.0] - 2026-02-13
### Changed
- `entity! { ... }` (without an explicit `id @` prefix) now derives a deterministic
  intrinsic entity id from its attribute/value pairs. Use an explicit id expression
  (for example `&ufoid() @`, `rngid() @`, or `genid() @`) when you want a fresh,
  extrinsic identity per invocation.
- `entity!` now accepts the explicit `_ @` prefix as a synonym for the derived-id
  behavior (useful when you want to be explicit in code reviews).

## [0.12.2] - 2026-02-10
### Changed
- PATCH tagged pointers now store node tags in the low 4 bits (16-byte aligned bodies)
  and keep the per-child key byte in the top byte, freeing address bits for larger
  virtual address spaces.

### Fixed
- PATCH no longer performs x86_64 sign-extension when decoding tagged pointers,
  avoiding incorrect canonicalization on systems with wider virtual addresses.

## [0.12.1] - 2026-02-10
### Fixed
- Fixed a Linux/aarch64 crash in `PATCH::clone()` when decoding tagged pointers.

## [0.12.0] - 2026-02-09
### Changed
- `Repository::create_branch` now mints branch ids with `genid` (high-entropy random ids) instead of `ufoid` (time-prefixed ids).

## [0.11.0] - 2026-02-08
### Added
- Branch tombstone records in the pile format for explicit branch deletion.

### Changed
- `BranchStore::update` now takes `new: Option<Handle<..>>`; `None` deletes the branch.
- `Pile` applies tombstones by removing branch heads from its in-memory branch index.
- `ObjectStoreRemote` represents tombstones as empty branch objects (size=0) and filters them out of `branches()`.

## [0.10.0] - 2026-02-07
### Added
- Attribute usage annotations with `metadata::attribute`,
  `metadata::source_module`, and `KIND_ATTRIBUTE_USAGE` for capturing
  contextual names/descriptions.

### Changed
- Clarified `metadata::name` and `metadata::description` as general-purpose
  entity naming/description attributes in docs and metadata comments.
- `Attribute::describe` now emits usage annotations when available, and the
  `attributes!` macro attaches contextual usage metadata (name/description/
  source) to declared attributes.
- Attribute usage ids are now anchored on the attribute id + module path only,
  so renames and file/line shifts do not churn usage identities.
- JSON importers validate UTF-8 strings via `View<str>` while reusing the
  parsed bytes.

## [0.9.0] - 2026-02-03
### Added
- Lossless JSON importer that preserves structure and ordering with explicit
  node/entry entities and content-addressed ids.
- `FileBytes` blob schema for explicit file-backed byte payloads.

### Changed
- Removed the serde-based and non-deterministic JSON importers; the remaining
  deterministic importer is now `JsonObjectImporter`.
- Renamed JSON importers for clarity: `JsonImporter` -> `JsonObjectImporter`,
  `LosslessWinnowJsonImporter` -> `JsonTreeImporter`, and `json_lossless` ->
  `json_tree`.

### Fixed
- Added the missing `Inline` import in the lossless JSON importer.

## [0.8.0] - 2026-01-22
### Added
- `TribleSetFingerprint` plus `TribleSet::fingerprint` for fast, in-process
  cache keys that match `TribleSet` equality.
- `Workspace::commit` now accepts optional commit metadata, plus
  `Workspace::checkout_metadata` and `Workspace::checkout_with_metadata` for
  reading commit metadata `TribleSet`s. Supplying commit metadata does not
  modify the workspace default.
- `Repository::set_default_metadata` and `Workspace::set_default_metadata`
  for configuring default commit metadata handles, plus
  `Repository::pull_with_metadata` for per-workspace overrides.
- `Repository::storage` and `Repository::storage_mut` for direct access to the
  underlying storage backend.
### Changed
- Renamed `WasmFormatterLimits` to `WasmLimits`.
- Commits can now carry an optional `metadata` handle alongside `content`.
- `repo::commit::commit_metadata` now takes an optional metadata handle instead
  of a metadata blob.
- `CommitSelector` ranges now treat branches without a head commit as empty
  history, so `Workspace::checkout` returns an empty `TribleSet` instead of an
  error when no commits exist.
- JSON importers now include schema metadata in their emitted metadata sets so
  value formatter lookups can succeed.

## [0.7.0] - 2026-01-18
### Changed
- Updated the README quote to Joe Armstrong’s talk “The Mess We’re In.”
- `Metadata` and `ConstMetadata` now use a shared `id` method as the canonical
  schema identifier, eliminating the former `metadata_id` accessors.
- `Metadata::describe` and `ConstMetadata::describe` are now fallible so blob
  write errors can be propagated instead of silently ignored.
- `InlineEncoding` inherits its identifier and default description behavior from
  `ConstMetadata`, removing duplicate `id`, `metadata_id`, and `describe`
  methods from the schema trait itself.
- Hash protocol metadata now emits the protocol name alongside the identifier
  so descriptions include the declared `NAME` for each digest.
- Replaced the `SchemaMetadata` helper with direct `ConstMetadata` impls on
  value schemas so static metadata stays in sync with runtime metadata roots.
- Removed explicit blob schema hooks from value schemas and attribute metadata,
  relying on metadata identifiers instead of nested blob schema entries.
- Updated schema documentation to reflect metadata-driven identifiers and
  examples that call `ConstMetadata::id()`.
- `Handle` value schemas now forward metadata from their hash protocol and blob
  schema components so composite schema descriptions stay discoverable.
- Removed the WASM module byte-size limit checks from module compilation and
  formatter loading; callers can enforce limits by checking blob sizes before
  compilation.
- `triblespace_core::wasm::shared_engine` is no longer part of the public API;
  module compilation uses an internal, lazy-initialized engine.
- Hash/handle WASM value formatters now always use the generic hex formatter,
  instead of special-casing specific hash protocols.
- Hash/handle WASM formatter output now prefixes `hash:` before the hex digest.
- Inlined schema-level value formatter metadata emission, removing the
  `wasm_formatters` helper module.
- Metadata describe helpers now bind schema IDs once and inline blob puts when
  emitting tribles.
- `entity!` now accepts `ExclusiveId` values in addition to `&ExclusiveId`
  references.
- Renamed `ExclusiveId::as_transmute_force` to `ExclusiveId::force_ref`.
- WASM value formatter limits are now supplied per formatting call (with a
  default helper), and the eager formatter loader no longer captures limits at
  construction time.
- `WasmModuleResolver` has been removed in favor of the generic `BlobCache`.
- `WasmValueFormatterResolver` has been removed in favor of direct
  `metadata::value_formatter` lookups with `BlobCache`.
- `load_wasm_value_formatters` has been removed in favor of `BlobCache` and
  `metadata::value_formatter` lookups.
- `#[value_formatter]` can override the generated WASM byte constant name and
  visibility via `const_wasm = NAME` and `vis(...)` arguments.
- Attribute identifiers derived from hashed names now use the rightmost 16 bytes
  of the Blake3 digest to stay consistent with the ID-to-value layout.
- Consolidated JSON import into a single deterministic
  `import::json::JsonObjectImporter` with fixed primitive mappings and optional salt
  support, replacing the prior nondeterministic importer and configurable
  encoder callbacks.
### Added
- Guidance on how `ExclusiveId` ownership narrows safe absence checks while
  keeping queries monotonic across collaborators in the incremental queries
  chapter of the book.
- `metadata::KIND_INLINE_ENCODING` and `metadata::KIND_BLOB_ENCODING` tags, now
  emitted by built-in schema metadata for discovery.
- `metadata::description`, a LongString-backed attribute for schema
  documentation, and `metadata::name`/`metadata::description` emission for
  built-in value and blob schemas.
- `metadata::Metadata` trait for emitting self-describing `TribleSet` and
  `MemoryBlobStore` pairs, enabling attributes and schemas to publish
  documentation metadata recursively.
- `TryToInline` implementations that convert `serde_json::Number` directly into
  the `F256` schema so JSON import code can call `.to_inline()` instead of
  hand-packing high-precision floats.
- Criterion benchmark covering deterministic JSON import performance using the
  serde-rs/json-benchmark fixtures.
- `GenId` value schema conversions for `uuid::Uuid`, including fallible packing and support for nil UUID values
  and unpacking helpers that enforce the non-nil invariant.
- Bundled the `canada.json`, `citm_catalog.json`, and `twitter.json` datasets to
  keep the JSON import benchmark self-contained.
- `import::json::JsonObjectImporter` for deterministic JSON imports that map strings
  to `Handle<Blake3, LongString>`, numbers to `F256`, booleans to `Boolean`,
  and nested objects to `GenId` links, hashing attribute/value pairs (with an
  optional 32-byte salt) to derive stable entity ids, while streaming blobs into
  a caller-provided store and exposing data/metadata separately.
- `inlineencodings::Boolean` for encoding `false` as all-zero bytes and `true` as
  all ones, providing an unambiguous target for JSON boolean importers.
- `RangeU128` and `RangeInclusiveU128` value schemas for encoding pairs of
  packed `u128` values, enabling compact storage of start/end markers such as
  source ranges.
- `LineLocation` value schema for storing explicit `(line, column)` start and
  end coordinates without manual packing, now used by the macro metadata
  instrumentation when recording invocation spans.
- `wasm` feature flag that bundles WebAssembly value formatters for the built-in
  value schemas and attaches them via `metadata::value_formatter` when emitting
  schema metadata.
- `#[value_formatter]` proc macro support in `triblespace-core-macros`, enabling
  the core crate to compile and embed formatter modules without introducing a
  dependency cycle.
- `triblespace-macros` crate wrapping the procedural macros and query helpers
  to record invocation metadata in an optional repository configured via the
  `TRIBLESPACE_METADATA_PILE` and `TRIBLESPACE_METADATA_BRANCH` environment
  variables.
- `TRIBLESPACE_METADATA_SIGNING_KEY` environment variable for configuring the
  signing key used when committing metadata; instrumentation skips emission when
  the value is unset or invalid.
- `Id::from_hex` helper for parsing hexadecimal identifiers, now reused by the
  macro metadata instrumentation when decoding branch IDs.
- Attribute definition metadata emitted alongside `attributes!` expansions,
  recording attribute identifiers, names, invocation IDs, and the declared
  schema type tokens for downstream analysis tools.
- Runtime helper `Attribute::from_name` for deriving deterministic attribute IDs
  from dynamic field names using schema metadata and hashed field handles.
- Shared `proofs::util` module providing bounded Kani generators for tribles,
  PATCH entries, and small commit DAGs, and updated the query harness to reuse
  them.
- `metadata::value_formatter` and `blobencodings::WasmCode` for attaching
  schema-level WebAssembly value formatters, plus an optional `wasm` feature
  (enabled by default in the `triblespace` facade crate) that runs them in a
  sandboxed `wasmi` interpreter with strict limits.
- `BlobCache`, a generic handle-keyed cache for blob conversions.
- `#[value_formatter]` proc macro attribute (in `triblespace-macros`) that
  compiles standalone Rust formatter functions into sandboxed WebAssembly
  modules and embeds the resulting bytes in the caller crate.
- Repository ancestor harness exercising `CommitSelector::ancestors` against
  nondeterministic DAGs generated by the shared bounded helpers.
- PATCH harness verifying entry insertion and replacement using the shared
  bounded generators.
- Documented the deterministic JSON importer flow and added regression tests
  covering integration with the fixed primitive mappings.
- Added a book chapter on importing external data formats that covers the
  JSON importers, attribute derivation, encoder callbacks, and strategies for
  extending the namespace to new formats.
- 0.6.0 release preparation checklist in `INVENTORY.md` detailing actionable
  subtasks for the remaining blockers and polish items.
- Formal verification roadmap outlining Kani, Miri, fuzzing, and simulation
  testing plans in the book.
- Expanded the roadmap with an explicit invariant catalogue, spelling out the
  PATCH/ByteTable invariants exercised by `Branch::modify_child`, clarifying the
  value-schema guarantees around `TryFromInline`, and synchronised follow-up
  tasks in `INVENTORY.md`.
  PATCH/ByteTable invariants exercised by `Branch::modify_child`, and synchronised
  follow-up tasks in `INVENTORY.md`.
- Documented the set-combinator commit selectors (`union`, `intersect`,
  `difference`) in the book with usage examples.
- `_?ident` scoped variables for `pattern!` and `pattern_changes!`, enabling
  fresh bindings without declaring them in `find!` heads, along with
  documentation and tests.
- `temp!` macro for allocating hidden query variables across constraints, plus
  documentation and regression tests covering cross-pattern joins.
- Clarified the `and!` example in the Query Language chapter to show how
  membership helpers can pair with pattern constraints drawn from a different
  collection.
- Documented repository storage backends and added a book page tracking future
  documentation improvements.
- Clarified the `Trible` layout, indexing strategy, and edge semantics in the
  deep dive chapter of the book.
- Expanded the documentation backlog with notes on remote object-store conflict
  handling, succinct archive indexes, and extending regular path engines.
### Changed
- `Attribute` now retains its declared name, uses the field name for dynamic
  attributes, and relies on the `Metadata` trait to emit attribute metadata in
  both code-generated and runtime scenarios.
- Simplified the attribute constructors to `from_id`, `from_id_with_usage`, and
  `from_name`, removing `from_id_with_name`/`from_handle` in favor of explicit
  usage metadata and internal handle derivation.
- Simplified attribute naming by replacing the internal `AttributeName` enum
  with an optional `Cow<'static, str>`, keeping const-friendly static ids while
  storing dynamic field names directly.
- Replaced the `InlineEncoding::VALUE_SCHEMA_ID` and `BlobEncoding::BLOB_SCHEMA_ID`
  associated constants with `ConstMetadata::id()` across value and blob schemas,
  preserving existing identifiers and deriving composite `Handle` schema IDs
  deterministically from their hash protocol and blob schema components.
- Made `HashProtocol` extend `ConstMetadata` so protocol identifiers come from
  the unified metadata API alongside value and blob schemas.
- Documented why schema identifiers remain regular functions until `blake3`
  exposes a const-friendly hashing API for composite handle schemas.
- Removed the `InlineEncoding::BLOB_SCHEMA_ID` associated constant and stopped
  emitting attribute metadata that relied on blob schema coupling.
- Glossary chapter in the book for quick reference to core terminology.
- Expanded the Identifiers chapter with a `local_ids` + `IdOwner` workflow
  example showing how to borrow freshly minted IDs in queries.
- `nth_ancestor` commit selector corresponding to Git's `A~N` syntax and
  documentation updates.
- `parents` commit selector corresponding to Git's `A^@` syntax.
- `INVENTORY.md` file and instructions for recording future work.
- README now links to the corresponding chapters on https://triblespace.github.io/triblespace-rs.
- `Constraint::influence` method for identifying dependent variables.
- Documentation and examples for the repository API.
- Book section showing how to stage and fetch workspace blobs with `Workspace::put`
  and `Workspace::get`.
- Guidance on integrating custom constraints with external data sources in the book.
- Garbage-collection chapter now shows how `BranchStore::branches`, `reachable`,
  and `transfer` work together to enumerate branch roots and traverse blobs in
  practice.
- Clarified the garbage-collection root description to highlight that the
  traversal retains everything reachable from enumerated branch metadata.
- Remote store workflow example in the book showing how to open
  `ObjectStoreRemote` repositories and clarifying that no explicit close is
  required for remote backends.
- `union`, `intersect`, and `difference` commit selectors that expose PATCH set
  operations through the `CommitSelector` interface.
- Documented `TribleSet` set operations and monotonic semantics in the Trible
  Structure chapter.
- Test coverage for `branch_from` and `pull_with_key`.
- Migrated `SuccinctArchive` to new `jerky`/`anybytes` APIs and added
  serializable metadata.
- `_?name` scoped variables for `pattern!`/`pattern_changes!` along with
  documentation and tests demonstrating their use.
- Implemented `ToBlob`/`TryFromBlob` for `SuccinctArchive`, enabling archive
  serialization as a blob.
- `Pile::restore` method to repair piles with trailing corruption.
- Documented zero-length blob support and added tests for empty blob insertion and retrieval.
- `with_sorted_dedup` constructor for universes to build from already sorted,
  deduplicated value sequences.
- Troubleshooting table in the repository workflows chapter covering common
  push, branch, and pull failure modes.
- Book section documenting how to manage multiple signing identities with
  `Repository::set_signing_key`, `Repository::create_branch_with_key`, and
  `Repository::pull_with_key`.
- Reworked Chapter 1 introduction to clarify Trible Space's goals, distinguish
  fixed-width tribles from blob payloads, and guide readers through the rest of
  the book.
- Dedicated "Portability & Common Formats" chapter in the book capturing value
  schemas, identifiers, and conversion guidance, referenced from the `Inline`
  crate docs, and closing out the documentation backlog request to move this
  material out of the API reference.
- Chapter exploring the TribleSpace type algebra linking `attributes!`,
  `entity!`, and query semantics.

### Changed
- Expanded the Pile Blob Metadata chapter with an accurate header field
  breakdown, timestamp conversion example, and details on lazy validation.
- Corrected the BlobMetadata description to state it surfaces the timestamp and
  length fields from the header.
- Expanded the Pile Format chapter with a restore-first operational workflow,
  corrected usage example showing a restore-after-open startup without an
  explicit refresh, and detailed record field breakdowns.
- Clarified that `Pile::restore` already applies intact records before
  truncating and that reader/branch helpers refresh automatically, so manual
  refreshes are only needed when scanning between operations.
- Corrected the PATCH deep-dive chapter so its descriptions of persistence,
  node layout, resizing, and hash maintenance match the implementation.
- Clarified the PATCH deep-dive resizing description to avoid implying growth
  beyond the 256-entry table.
- Clarified the PATCH hash-maintenance discussion so hash comparisons
  short-circuit on matches and walk on mismatches.
- Expanded the deep-dive blobs chapter with guidance on when to use blob
  storage, how handles relate to schemas, and annotated examples.
- Corrected the Trible Structure deep dive to describe how `TribleSet::union`
  mutates its receiver while the other set operations return new views.
- Expanded the deep-dive philosophy chapter with explicit guiding principles,
  clarified how asynchronous backends surface through blocking entry points, and
  grounded the practical implications in the currently available tooling.
- Expanded the Formal Verification roadmap with a stack overview, contributor
  workflow guidance, and milestone tracking suggestions.
- Expanded the garbage-collection chapter with guidance on choosing root sets,
  operational tips, clearer explanations of the conservative traversal, and a
  scan description that matches the implementation (including the fact that the
  walker scans every blob in 32-byte chunks because the store is type-agnostic).
- Reworded the garbage-collection safety-margin tip to emphasize that
  near-impossible hash collisions make extra roots a conservative way to protect
  reachable data.
- Refined the garbage-collection example loop to iterate branch IDs directly
  when collecting roots from `BranchStore::branches`.
- Reworked the documentation improvement chapter with prioritised sections and
  contribution guidance for future book updates.
- Standardised citation formatting in the documentation backlog to match the
  book's reference style.
- Refined the Descriptive Typing chapter with accurate workspace lifecycle
  guidance, corrected `find!` pattern syntax, clearer advice on structuring
  ad-hoc projections and strongly discouraging long-lived typed wrappers,
  updated the
  manager-owned repository DI section to emphasize using short-lived
  `&mut Repository<_>` borrows, handing out task-scoped `&mut Workspace<_>`
  handles, highlighting how multiple mutable workspaces coexist over a
  single repository, clarified that cloning an already-fetched blob is cheap
  compared to the cost of retrieving it from storage, and corrected the
  description of composable clauses so it no longer suggests optional
  attribute matching.
- `json_import` benchmark now publishes separate element- and byte-throughput
  groups, precomputing importer-specific trible counts so Criterion reports
  both perspectives.
- Inlined the JSON importer's trible insertion helper to avoid an extra
  function hop when staging statements.
- Updated both JSON importers to stage objects as `TribleSet`s and have the
  entrypoints union the staged results after validation, removing the
  `PendingJsonObject` scaffolding.
- Inlined the deterministic JSON importer's raw trible helper so hashed
  statements insert without bouncing through an additional function.
- Constructed deterministic JSON importer statements with `Trible::new`
  instead of hand-assembling raw buffers each time a pair flushes.
- Restored the JSON importer's `PrimitiveRoot` error for non-object roots,
  dropped the deterministic importer nil-id guard, and added regression tests
  covering both cases.
- Allowed both JSON importers to accept top-level arrays by returning multiple
  root entities, keeping primitive roots rejected while permitting batches of
  objects.
- Simplified the JSON importer API to return root ids while exposing data and
  metadata via accessors on `JsonObjectImporter`, avoiding an extra wrapper type.
- Simplified JSON importer error diagnostics to avoid tracking JSON paths in
  the hot import loop.
- JSON importers now emit `metadata::name` and `metadata::attr_value_schema`
  tribles when minting attributes so imported datasets carry their own schema
  descriptions.
- Attribute metadata emission now uses the public `entity!` macro so schema
  descriptions are assembled with the same ergonomic syntax exposed to
  consumers.
- Both JSON importers now merge their cached attribute metadata into the
  result set after converting documents instead of inserting metadata entries
  mid-import, keeping the hot path lean while still returning the schema
  descriptors for every derived attribute.
- JSON importers now compute metadata tribles directly from their attribute
  caches at the end of each import, avoiding duplicate metadata storage while
  preserving the descriptors for all derived fields.
- Clarified the importing guide to explain that `metadata()` returns attribute
  descriptors generated from the cached ids after an import completes.
- Documented how deterministic JSON imports collapse repeated subdocuments,
  explaining why they can outperform nondeterministic runs even with cheap ID
  generators.
- JSON importers retain the accumulated tribles inside the importer, exposing
  `data()`/`metadata()` accessors along with `clear_data()` and `clear()` helpers
  so multiple JSON documents can be staged—or reset entirely—before reading the
  results instead of returning a fresh `TribleSet` from each `import_value`
  call.
- Replaced the JSON importer's `anyhow` dependency with a lightweight encoder
  error wrapper so callbacks stay flexible without pulling in extra baggage.
- Dropped the JSON importer's `JsonValueKind` helper and folded the top-level
  type detection directly into the error path to trim unused indirection.
- Parameterized the JSON importer's ID generation so callers can provide
  deterministic sequences via `with_id_generator`, and added a regression test
  covering custom generators.
- Cached JSON importer attributes per field name so repeated values reuse the
  same hashed identifiers without recomputing them.
- Expanded the Schemas chapter with validation examples, clarified how schema
  identifiers power cross-language tooling and deterministic attribute imports,
  outlined schema evolution best practices, and corrected the built-in blob
  schema references for succinct archives.
- Expanded the Incremental Queries chapter with practical guidance on
  preparing delta sets, reusing `TribleSet` set operations, and tying the
  workspace and local-buffer stories together through the shared set
  algebra that powers both workflows.
- Expanded and corrected the Atreides Join chapter with a structured
  walkthrough: it now explains the constraint interface, details the
  Jessica/Paul/Ghanima/Leto heuristic ladder, clarifies what quantity each
  variant estimates, describes the ordering heuristics used by the guided
  search, motivates the worst-case optimal guarantee, and clarifies how
  per-variable estimates are derived in the worked example while tying the
  introduction back to the broader worst-case optimal join literature.
- Macro instrumentation now records the entire span of each invocation in a
  single `source_range` attribute instead of separate line and column values.
- Implemented `ToEncoded<LineLocation>` for `proc_macro::Span` so metadata
  wrappers can hand spans directly to `entity!` without manual tuple
  construction.
- Attribute metadata emission no longer attempts to resolve value/blob schema
  identifiers, sticking to the information reliably available at macro
  expansion time.
- Metadata emission callbacks now receive a mutable context exposing the
  workspace, invocation ID, and tokens so wrapper macros can commit additional
  metadata directly without reopening the repository.
- Metadata emission now commits records to the configured repository branch
  instead of appending raw archives to a standalone pile, aligning the
  instrumentation with the standard storage workflow and renaming the
  environment variable knobs accordingly.
- Regenerated the macro instrumentation attribute identifiers from
  command-line randomness to document their provenance and avoid
  hand-crafted values.
- Metadata instrumentation now reuses the shared hex parsing helpers when
  decoding signing keys and branch identifiers from the environment and
  requires exact hexadecimal strings without a prefix, eliminating bespoke
  sanitization logic in the wrapper crate.
- Reworked the Query Engine chapter to describe the in-search Atreides
  cardinality estimates, clarify how constraints cooperate at runtime, and remove
  references to a nonexistent planner.
- Clarified how the Query Engine search loop derives join variants from
  cardinality heuristics, documented the role of `confirm` inside `and!`, and
  replaced the chapter's query example with a runnable snippet that mixes
  `pattern!` constraints with a `HashSet` filter.
- Updated the architecture overview and trible structure deep dive so they
  describe join ordering as a search-loop choice driven by constraint
  heuristics instead of a separate planner.
- Clarified in the Architecture chapter that blob stores, not repositories, perform
  deduplication of uploaded content.
- Corrected the push/pull arrows in the Architecture diagram to match the actual
  workspace and repository data flow.
- Refined the Architecture diagram and explanation to match
  `Repository::pull`, `Workspace::commit`, and `Repository::try_push`
  responsibilities.
- Reworked the Architecture diagram again to restore the approachable
  workspace overview, clarify the `commit`/`add_blob` interactions, and ensure
  the push arrow flows from the workspace into the repository box.
- Tightened the Architecture diagram so `push/try_push` rises from the
  workspace, `pull` flows back from the repository, and the workspace box now
  highlights concise `commit`/`add_blob` annotations plus a `checkout` link to
  the application layer, then nudged the arrow spacing and arrowheads for
  clearer alignment.
- Re-reviewed the book and codebase to tighten the Glossary definitions:
  clarified how attributes carry their schemas via `attributes!`, explained
  that schemas stay language agnostic instead of binding to Rust types, noted
  that blobs hold archived `TribleSet`s and commit metadata, documented commits
  as `SimpleArchive` blobs with signed metadata, and highlighted identifier
  ownership in the entity entry alongside the existing PATCH description.
- Reorganized the workspace so the new `triblespace` crate exposes the public
  prelude, examples, and documentation while the implementation lives in
  `triblespace-core` with procedural macros in `triblespace-core-macros`,
  enabling future proc-macro crates to depend on the core without cyclic
  dependencies.
- Moved the README regression test and Kani proof harnesses into the
  `triblespace` facade crate so `triblespace-core` stays lean for proc-macro
  consumers while the public API remains thoroughly exercised.
- Expanded Chapter 1 of the book with clearer motivation, reader guidance, and
  an outline of the subsequent chapters. Streamlined the "Why Trible Space
  exists" section so it stays focused on the data-management pains Trible Space
  solves and how pairing blobs with fine-grained facts addresses them, and
  reworded the flexible querying description to show how a single query blends
  trible sets, succinct indexes, and Rust collections such as hash maps.
- Aligned the README regression test with the expanded library conflict resolution walkthrough so documentation stays exercised.
- Regenerated the quick-start alias attribute ID with a CLI-generated value so the README, book, and regression test stay in sync.
- Unified the getting started walkthrough around the library example, showing `push` for automatic retries, `try_push` for manual conflict handling, and updating the README snippet to match.
- Expanded the book's getting started chapter with clearer step-by-step setup,
  execution instructions, and explanations of the repository workflow pieces.
- Restored the README's quick-start example while keeping the expanded
  walkthrough in the getting started chapter so newcomers can skim or dive
  deeper as they prefer.
- Contributor guidelines now require reading the entire `./book` before starting each new task to stay aligned with project concepts.
- `proofs::util::bounded_id` now rejects the nil sentinel with `kani::assume`
  to keep identifier generation unbiased while ensuring exclusivity checks stay
  sound in verification harnesses.
- Expanded the Developing Locally chapter with setup steps, workflow scripts,
  and book rebuild instructions.
- Reconciled the duplicated Query Language edits by combining the reorganised
  introduction, conversion guidance, simplified `ignore!` syntax that always
  captures the surrounding query context while still minting distinct
  temporary variables, richer `or!` and `pattern!` examples, and updated
  regular path query coverage.
- Clarified the regular path example to use `temp!` when hiding an endpoint so
  the traversal still participates in follow-up constraints without projecting
  the hidden binding.
- `ignore!` now always infers its context from `find!`/`exists!`. Use
  [`IgnoreConstraint::new`](https://docs.rs/tribles/latest/tribles/query/ignore/struct.IgnoreConstraint.html)
  directly when building bespoke constraints outside those macros.
- `temp!` now mirrors `ignore!` by taking both the tuple-style binding list and
  the scoped expression, so helper variables introduce their own temporary
  block without wrapping the surrounding query body manually.
- `temp!` no longer accepts explicit type annotations. Hidden bindings never
  project into the result tuple, so their value schemas are inferred entirely
  from how they are used inside the scoped expression.
- Documented `temp!` alongside the other built-in macros in the Query Language
  chapter's constraint table so readers can spot it at a glance.
- Clarified the `ignore!` documentation to highlight that ignored bindings are
  never solved or unified, showing how triple-style constraints can drop unused
  positions while branches that reference only ignored variables never even get
  scheduled.
- Streamlined the `ignore!` partial-projection example by trimming unrelated
  namespace discussion and added an introduction note that highlights how the
  macros wrap the underlying constraint builders for manual use.
- Query Language chapter now gives `or!` its own subsection, calls out
  `_?name` placeholders in `pattern!`/`pattern_changes!` as an alternative to
  `temp!` when hidden helpers stay within a single pattern, clarifies that each
  branch behaves as an independent constraint whose matches are all retained so
  the overall query stays monotonic, documents that all branches must reference
  the same variable set, and notes that mismatches panic at runtime.
- Documented the `.is(...)` constant constraint alongside the other built-in
  operators, added a dedicated subsection showing how to pin bindings,
  highlighted that `pattern!`/`pattern_changes!` already materialise constant
  constraints for literal values, and pointed readers to membership helpers
  such as `.has(...)` when accepting several literals.
- Added `pattern!` and `pattern_changes!` to the built-in constraints table,
  noting that incremental patterns emit only additions and pointing readers to
  the Incremental Queries chapter for the full evaluation workflow.
- Clarified the `has` membership entry so it points to `ContainsConstraint`
  implementors like set-style collections while steering triple sources toward
  `pattern!`.
- Added an "Intersections (`and!`)" subsection to the Query Language chapter
  covering how conjunctions combine clauses, share bindings, and nest within
  other combinators.
- Normalized the Descriptive Typing chapter to use consistent Markdown headings
  and remove unused front matter.
- Re-reviewed the type algebra chapter, linking its claims directly to the
  `Attribute`, `TribleSet`, and query constraint implementations for accuracy.
- Softened the Type Algebra chapter summary to describe the design without
  value-laden language.
- Rephrased the Type Algebra chapter's closing sentence to highlight surface
  simplicity backed by rich type theory.
- Clarified `PATCH::iter_ordered` and `PATCHOrderedIterator` documentation to
  describe the full tree-order traversal without a prefix filter and point to
  the prefix iterator for filtered traversal.
- Reframed the identifiers deep-dive chapter to highlight the abstract/semantic
  and intrinsic/extrinsic axes, expand the embeddings discussion, and provide
  clearer guidance on choosing identifier families.
- Audited the identifier taxonomy guidance to align the RNGID/UFOID/FUCID
  comparison with their implementations and fix crate-qualified links in the
  table.
- Trimmed the Portability & Common Formats chapter by removing the "Why this
  chapter lives in the book" subsection after documenting the move from the
  `Inline` module docs.
- Documented the `path!`, `attributes!`, and `pattern_changes!` procedural
  macros in the `tribles-macros` crate overview.
- `attributes!` procedural macro now resolves the caller's crate path so
  downstream users can depend solely on the `triblespace` facade when
  generating attribute constants.
- Reframed commit range selectors so `start..end` walks from the end selector
  until encountering a commit yielded by the start selector, reducing
  redundant ancestor exploration and making the traversal cost explicit.
- Query Engine chapter now directs readers to the crate-level `pattern!` and
  `entity!` macros and shows how to import them via the prelude.
- Removed the outdated note that parentheses "force" literals in the getting
  started guide now that the macros rely on regular Rust expression syntax for
  literal detection.
- Commit selectors chapter now highlights range semantics, composability, and
  Git parity to help readers choose the right selector for their workflow,
  clarifies that selectors only pick commits while `Workspace::checkout`
  materializes the `TribleSet`, refreshes the composition example to layer
  entity filters over a time range, and shows how to combine selectors with the
  built-in set-operation helpers.
- Pinned `anybytes` and `jerky` to specific git revisions via a crates.io patch
  so all dependents use a single source and API surface.
- Refined the selector debugging guidance to encourage validating each layer
  independently before composing them with the built-in set-operation helpers.
- Documented the trade-off that empty start selectors rewalk the full history,
  and showed how incremental queries can reuse the previous head commit as the
  next range boundary to avoid repeating the walk.
- Corrected the commit selector range description to note inclusive end
  boundaries and clarified that selectors compose via the `CommitSelector`
  trait instead of `IntoIterator`.
- Clarified the commit selector traversal description to avoid implying a
  specific order, fixed the `ancestors(A)..B` exclusion example, and tightened
  the debugging guidance wording.
- Clarified that `find!` retrieves `ExclusiveId` bindings via `TryFromInline` and
  that restricting queries with `local_ids` keeps the conversion safe.
- Getting started guide now demonstrates defining custom attributes alongside
  the quick-start example, hides doc-test-only cleanup, and exercises the
  quick-start snippet as a runnable doc test.
- Updated README and book code samples to use the public `entity!`/`pattern!`
  macros so snippets copy-and-paste outside the crate.
- Updated the README and book examples to use `Repository::create_branch` plus
  `pull` instead of the removed `branch` helper when initializing workspaces.
- Combined the README quick-start and standalone example into one repository
  workflow that stages, queries, and pushes a dataset backed by freshly minted
  `attributes!` definitions instead of the shared literature namespace.
- Updated the release preparation inventory to call out multi-`attributes!`
  module examples instead of cross-namespace guidance.
- Pruned completed 0.6.0 release checklist items (prefix guards, succinct archive parity,
  pile property tests) from the inventory after auditing the codebase.
  - README walkthrough and regression test now commit the staged dataset by value
    instead of cloning it before submission.
  - Updated `SuccinctArchive` to use `BitVectorDataMeta` for prefix bit vectors.

### Fixed
- Reinstated the `InlineEncoding` documentation that notes hash handles still carry
  their referenced blob schema type parameter.
- Updated deterministic JSON importer metadata tests to align with attribute
  metadata now emitting only value schema descriptors.
- Added the missing `blake3` dev-dependency and adjusted the JSON importer
  benchmark to allocate owned strings and convert JSON numbers via
  `f256::from`, restoring the json benchmarks after recent refactors.
- Updated JSON importer benchmarks, core tests, and book snippets to ensure the
  `LongString` generic parameter stays attached to the trait, fixing
  compilation failures introduced by the new benchmark and documentation
  examples. Book snippets now rely on type inference for `to_blob()` to match
  idiomatic usage.
- Corrected the JSON import benchmark to use the re-exported
  `inlineencodings::Blake3` handle schema so it compiles again.
- Added the missing `serde_json` and `f256` dev-dependencies so the JSON import
  benchmark builds successfully.
- Buffered the JSON importers so encoding errors roll back an entire import
  instead of leaving partially imported tribles in the accumulated set.
- Routed the JSON importer staging helpers through a shared temporary
  `TribleSet` so field emitters avoid building intermediate sets before the
  batch commits.
- Updated the procedural macros to resolve either the `triblespace-core` or
  `triblespace` crate path automatically so downstream users can rely on the
  facade crate without declaring extra dependencies.
- `SuccinctArchive` now derives domain metadata via `Serializable` instead of storing raw handles.
- `SuccinctArchive` now retains a handle to a contiguous byte area so blob serialization clones the underlying bytes without rebuilding.
- Simplified blob deserialization by reading archive metadata via `Bytes::view_suffix`.
- `SuccinctArchive`'s `Serializable` implementation now reports concrete
  `jerky::error::Error` values instead of relying on `anyhow`.
- Removed the custom empty `WaveletMatrix` metadata workaround now that the
  builder accepts zero-length sequences.
- `SuccinctArchive::from` now seeds wavelet matrices without guarding against
  empty archives because the builder handles zero-length iterators.
- Verified the wavelet-matrix builder path against empty archives via
  `./scripts/preflight.sh` after the jerky upgrade.
- `OrderedUniverse` now stores values as `View<[RawInline]>` for zero-copy access.
- Simplified `OrderedUniverse::with_sorted_dedup` to always collect incoming
  values before writing them into the reserved section, avoiding reliance on
  unstable iterator detection.
- Universes now allocate their own byte sections via a `SectionWriter`, so callers only pass an iterator. `CompressedUniverse::with` no longer clones its values.
- `SuccinctArchive` constructs universes with `with_sorted_dedup`, avoiding an extra sort/dedup pass when the caller already guarantees ordering.
- Updated the repository workflow documentation to use `Repository::create_branch`
  and provide a runnable blob staging example.
- Expanded the repository workflows chapter with an overview of repository
  initialization, branching conventions, and guidance on choosing between
  `push` and `try_push`.
- Getting started guide now highlights the need to close pile-backed repositories so callers can handle flush errors explicitly.
- README example now inlines the shared `tribles::examples::literature` namespace so the getting started walkthrough and crate examples stay aligned without depending on internal modules.
- README walkthrough and regression test keep the namespace name `literature` to match the shared example module.
- `with_sorted_dedup` now accepts iterators so compressed universes can build domains without materializing values.
- `SuccinctArchiveMeta` now accepts the domain's serialized metadata type,
  removing its hardcoded `SectionHandle<RawInline>` dependency.
- Architecture chapter now explains the system layers, copy-on-write behaviour,
  and how repositories coordinate blob and branch stores.
- `SuccinctArchiveMeta` bounds metadata types with jerky's `Metadata` marker
  to guarantee zero-copy-safe layouts.
- `CompressedUniverse` now relies solely on jerky's `DacsByte` and a section-
  backed fragment table, enabling fully zero-copy serialization via
  `Serializable`.
- Documented that branch updates do not ensure referenced blobs exist, enabling
  piles to serve as head-only stores.
- Clarified repository workflow docs with a sidebar highlighting
  `repo::transfer` alongside `BlobStoreKeep::keep`, including
  garbage-collection scenarios that only copy live blobs.
- Removed the suggested branch conventions subsection from the repository
  workflows chapter so the page concentrates on API behavior and storage
  guidance.
- Clarified that multiple pile writers require filesystems with atomic append
  semantics; noted unsupported filesystems in documentation.
- Streamlined the merge troubleshooting note to highlight
  `MergeError::DifferentRepos` and the `reachable` + `repo::transfer` steps for
  cross-repository merges.
- Documented the pile as a write-ahead log database ("WAL-as-a-DB").
- Rewrote the pile blob metadata chapter to describe the `BlobMetadata`
  API and linked it from the pile format documentation.
- Documented that the pile is an immutable append-only log: only the un-applied tail is validated and mutating existing data is undefined behavior.
- Removed in-flight blob tracking. `Pile::put` now holds a shared lock,
  refreshes before writing, then reads back its blob with `apply_next` to ensure
  it was indexed. `Pile::update` similarly verifies the written branch record
  using `apply_next` under its exclusive lock.
- `Pile::close` now consumes the pile and manually drops its fields to bypass
    `Drop`, which always warns when a pile is not explicitly closed.
- `Pile::close` now drops all fields before returning the result of `flush`,
  ensuring resources are cleaned up even if flushing fails.
- `Pile::refresh` now aborts if the pile file shrinks below data already
  applied, guarding against truncated data.
- Documented that truncation below `applied_length` invalidates previously
  issued `Bytes`, so only the un-applied tail is checked for corruption and
  shrinkage into validated data requires aborting.
- Clarified that shrinkage into already applied data triggers an immediate
  process abort to avoid undefined behavior from dangling `Bytes` handles.
- `Pile::refresh` acquires a shared file lock while scanning to avoid races with
  `restore` truncating the file.
- `Pile::restore` truncates the pile without rescanning after truncation,
  removing a redundant refresh pass.
- `Pile::refresh` uses a simple `insert` for new blob index entries.
- `Pile::update` no longer flushes or `sync_all`s automatically; callers must
    invoke `flush()` for durability.
- `Pile::open` now returns an empty handle without scanning the file. Call
  `refresh` to load existing data or `restore` to repair corruption. The
  `try_open` helper was removed.
- Additional unit tests for `Pile` blob iteration, metadata, and conflict handling.
- `Workspace::checkout` helper to load commit contents.
- Documentation and example for incremental queries using `pattern_changes!`
  plus additional tests.
- `pattern!` now implemented as a procedural macro in the new `tribles-macros` crate.
- Regression test ensuring `PATCHOrderedIterator` returns keys in sorted order.
- `entity!` now implemented as a procedural macro alongside `pattern!`.
- `ThompsonEngine` implementing a new `PathEngine` trait for regular path queries,
  and `RegularPathConstraint` is now generic over `PathEngine`.
- `reachable` iterator, `transfer` helper, and `potential_handles` expose the
  conservative blob traversal for composition. `BlobStoreKeep::keep` and
  `MemoryBlobStore::keep` now retain blobs by handle iterators.
- Implemented `size_hint`, `ExactSizeIterator`, and `FusedIterator` for `PATCHIterator` and `PATCHOrderedIterator`.
- Compile-time check restricting builds to 64-bit little-endian targets.
- `PileReader` now reconstructs blob data from the underlying memory map,
  and `IndexEntry::Stored` tracks offsets and lengths instead of holding `Bytes` directly.
- Regression test ensures `PATCH::iter_ordered` yields canonically ordered keys.
- `PATCH::replace` method replaces existing keys without removing/ reinserting.

### Fixed
- Corrected the repository workflow documentation to describe the actual
  `Repository::push` and `Repository::try_push` return values and clarify that
  remote backends expose a no-op `repo.close()`.
- Corrected the `PATCHOrderedIterator` documentation to describe its
  lexicographic key-order traversal instead of prefix iteration.
- Restored `_?ident` locals in `pattern!`/`pattern_changes!` to infer their
  value schema from usage instead of forcing `GenId`, so scoped bindings work on
  non-`GenId` attributes again.
- Resolved hygiene issues in `pattern!`/`pattern_changes!` so user bindings like
  `__ctx` no longer collide with generated identifiers, and added trybuild
  coverage to prevent regressions.
- Corrected the blob book example to import the repository module via `tribles::repo`.
- Removed an unused `anyhow` import from the succinct archive schema.
- `SuccinctArchive::from` now handles empty `TribleSet`s and returns an
  empty archive instead of panicking.
- `CachedUniverse::search` avoids underflow when querying an empty universe.
- Opening excessively large piles now returns an error instead of panicking when calculating the mapped size.
- Regression tests verify blob bytes remain intact after branch updates and across flushes.
- `PileReader::metadata` now validates blob contents and returns `None` for corrupted blobs.
- `PileBlobStoreIter` now lazily verifies blob hashes and reports errors for invalid blobs.
- `PileBlobStoreIter` now skips missing index entries instead of ending iteration silently.
- `Pile::flush` now calls `sync_all` to persist file metadata and prevent
  potential data loss after crashes.
- `Pile::restore` now syncs the file after truncation to ensure durability.
- `Pile` requires explicit closure via `close()`; dropping without closing emits a warning.
- Debug helpers `EstimateOverrideConstraint` and `DebugConstraint` moved to a new
  `debug` module.
- Debug-only `debug_branch_fill` method computes average PATCH branch fill
  percentages by node size.
- Added a simple `patch` benchmark filling the tree with fake data and printing
  branch occupancy averages.
- Trible key segmentation and ordering tables are now generated from a
  declarative segment layout, simplifying maintenance.
- Deterministic proptest simulation tests cover multi-reader and writer pile
  operation sequences via actor-scheduled operations.
- Simulation now exercises branch updates, branch listing, and fetching
  previously stored blobs and branch heads for comprehensive pile coverage.
- Additional pile unit tests exercising branch conflicts and size limits.
- Additional unit tests cover pile blob metadata, iteration, and branch update
  conflicts.
- Additional unit tests covering pile deduplication, metadata, and branch
  update conflicts.

- `Pile` no longer requires a compile-time size limit, grows its mmap on demand,
  and `ReadError::PileTooLarge` was removed.
- Initial pile mapping now uses a page-sized (×1024) base to avoid frequent remaps.
- Mapping size now derives from the mmap length instead of an internal counter.
- Replaced fs4 with Rust std file-locking APIs.
- Declared Rust 1.89 as the minimum supported toolchain.
- Dropped the inventory item about validating externally appended blobs during
  `refresh`; blob data is verified lazily on read.
- `refresh` replaces invalid blob entries with newer candidates and verifies
  unknown duplicates before deciding whether to keep or replace them.
- `refresh` now uses `get_or_init` to compute blob validation state and
  replace invalid duplicates.
- Simplified `refresh` padding logic by using `padding_for_blob` to compute blob alignment.
- `BlobStore::reader` now returns a `Result` so implementations can signal errors during reader creation.
- Renamed pile read errors from `OpenError` to `ReadError` since they can surface during refresh.
- PATCH exposes const helpers to derive segment maps and ordering
  permutations from a declarative key layout.
- `Entry` now supports an optional value via `with_value`, preparing `PATCH`
  for key-value mappings.
- Set semantics now use the zero-sized unit `()` value instead of a dummy
  byte to avoid extra storage.
- `PATCH::get` retrieves the value associated with a key, if present.
- `Leaf` stores the associated value and `PATCH`/`Head`/`Branch` now carry a
  value type parameter so keys can map to arbitrary payloads.
- Moved the value type parameter to the end of generic parameter lists for a
  more ergonomic `PATCH<KEY_LEN, Order, Inline>` API.
- Documented that hashing and equality ignore leaf values and added a
  regression test verifying patches with identical keys but different values
  compare equal.
- Introduced `key_segmentation!` and `key_schema!` macros to emit
  `KeySegmentation` and `KeySchema` implementations from those declarative
  layouts.
- Added `byte_table_resize_benchmark` measuring average fill ratios that cause
  growth for random vs sequential inserts. It now tracks the number of elements
  inserted at each power-of-two table size to compute per-size and overall
  averages over many random runs.
- Preallocated the resize counts vector to avoid repeated allocations during
  the benchmark.
- Per-size results now include sizes that never triggered growth so the output
  has no gaps.
- Documented PATCH's cuckoo-hashing compression as an alternative to ART-style
  node compression, explained its compressed-permutation hash with an identity
  first permutation and a random second permutation and why the smallest and
  largest nodes are always fully occupied, and included benchmark fill ratios in
  the book.
- Annotated the benchmark output to highlight path compression in the size-two
  case and that the identity hash lets 256-ary nodes store all 256 children.
- `entity!` subsumes the old `entity_inner!` helper; macro invocations can
  optionally provide an existing `TribleSet`.
- Procedural `namespace!` macro replaces the declarative `NS!` implementation.
- Implemented a procedural `delta!` macro for incremental query support.
- Expanded documentation for the `pattern` procedural macro to ease maintenance, including detailed comments inside the implementation.
- Expanded Query Language chapter with iterator examples and clarified that
  `ignore!` removes the named variables from planning while the scoped bindings
  still unify inside the ignored expression, making it easy to drop value
  columns from multi-position constraints without losing the join on the
  remaining variables.
- `EntityId` variants renamed to `Var` and `Lit` for consistency with field patterns.
- `Workspace::checkout` now accepts commit ranges for convenient history queries.
- Git-based terminology notes in the repository guide and a clearer workspace example.
- Expanded the repository example to store actual data and simplified the conflict loop.
- Failing test `ns_local_ids_bad_estimates_panics` shows mis-ordered variables return no results when a panic is expected.
- Diagram and explanation of six trible permutations and shared leaves for skew‑resistant joins.
- Additional example in the Commit Selectors chapter demonstrating how to
  compose `filter` with `time_range`.
### Changed
- `Branch::upsert_child` now always refreshes `childleaf`, removing the `replaced_leafchild` check.
- Blob index now uses value-aware `PATCH` for cheap reader clones.
- Inlined `refresh_range` logic into `refresh`, removing the partial-range helper.
- Blob appends now issue a single `write_vectored` `O_APPEND` call to stream header, data and padding without extra copies or retries.
- Simplified vectored blob appends by always including a padding slice.
- Branch updates now perform `flush → refresh → lock → refresh → append → unlock` directly instead of queuing.
- Branch headers are written with a single `write` call to avoid partial updates.
- Max-size checks and mmap offsets now derive from the file's actual length instead of tracked counters.
- Restored an `applied_length` tracker to incrementally refresh new blobs and branches without rescanning the entire pile.
- Blob inserts now compare the write start with the previous `applied_length`, ingesting any intervening records before advancing.
- `refresh` now uses the same framing parser as `try_open` to detect truncated or malformed records while deferring blob hash checks to reads.
- `try_open` now reuses `refresh` for log scanning, unifying corruption checks.
- `succinctarchive` schema is now gated behind an optional `succinct-archive`
  feature until it aligns with upstream `jerky` APIs.
- `refresh` retains existing blob entries when encountering duplicates instead of
  replacing validated records.
- `refresh` now uses `PATCH::replace` to update blob entries without explicit remove/insert.
- Expanded commit selector documentation with an overview, example and clearer
  wording about loading commits from a workspace.
- Temporarily gate the `SuccinctArchive` schema behind a feature to restore
  compilation while its Jerky dependency is updated.
- Expanded repository workflows chapter with clearer branching steps and a
  dedicated history section.
- Expanded Schemas chapter with additional context on schema identifiers and runtime lookup.
- Renamed `mask!` macro to `ignore!` for clarity.
- Expanded the Atreides Join chapter with an example, clearer algorithm explanations, and a note that random access remains only for confirming candidates.
- Rephrased Atreides Join discussion of sorted indexes to highlight efficient value lookup.
- Gave each Atreides join variant a descriptive name alongside its Dune nickname.
- Clarified the query engine book chapter with improved wording and examples.
- Expanded discussion on RDF's per-value typing limitations in the query engine chapter.
- Expanded Architecture chapter's blob storage section for clearer responsibilities and examples.
- Expanded the "Developing Locally" book chapter with guidance on helper scripts and local setup.
- Expanded the "Getting Started" book section with dependency setup and run instructions.
- PATCH infix and segment-length operations now require prefixes to align with
  segment boundaries.
- `KeySchema` and `KeySegmentation` now expose translation tables as associated const arrays instead of methods.
- Removed `key_index`, `tree_index`, and `segment` helper methods in favor of direct const-table lookups and tied `KeySchema` to its `KeySegmentation` with an explicit segment permutation.
- `KeySchema` now declares its `KeySegmentation` via an associated type instead of a separate generic parameter.
- Renamed `KeyOrdering` trait and `key_ordering!` macro to `KeySchema` and `key_schema!` for clearer terminology.
- Blob writes are now synchronous; `put` records an `InFlight` entry so repeated writes of the same blob are deduplicated until a refresh.
- Pile size limits are enforced during `refresh` rather than on each write.
- `ByteTable` plans insertions by recursively seeking a free slot and shifts entries only after a path is found, returning the entry on failure so callers can grow the table.
- ByteTable's planner tracks visited keys with a stack-allocated bitset to avoid heap allocations.
- Simplified the planner and table helpers for clearer ByteTable insertion code.
- Replaced redundant option check with an `expect` when traversing full buckets in
  the ByteTable planner.
- Restored the simpler `ByteSet` and inlined bucket checks to reduce indirection in the planner.
- Removed the reified `ByteBucket` abstraction and indexed buckets directly in the byte table.
- `ByteSet` now stores raw `[u128; 2]` bitsets instead of relying on `VariableSet`.
- Detailed query engine documentation moved from the `query` module to the book, leaving a concise overview in code.
- Moved verbose inline documentation for Pile, Trible, Blob and PATCH modules
  into the book.
- Expanded Trible Structure deep-dive with design rationale and advantages
  previously kept inline.
- Added remaining rationale from the blob, patch, pile and schema docs to the
  corresponding book chapters so code comments stay concise without losing
  detail.
- Expanded the incremental queries chapter with step-by-step delta evaluation
  and clearer `pattern_changes!` guidance.
- Refined the book's introduction with a clearer overview of Trible Space and
  its flexible, lightweight query engine, plus links to later chapters.
- Simplified blob length handling in `Pile::refresh` by relying on
  `take_prefix`'s implicit bounds checking.
### Removed
- `nth_parent` commit selector and helper; parent-numbering is not planned.
- Unused `crossbeam-channel` dependency.
### Fixed
- Detect oversized blob headers whose declared length exceeds the file size.
- Restored atomic vectored blob appends and single-call branch writes; errors
  if any bytes are missing.
- Removed duplicate `succinct-archive` feature declarations that prevented
  builds.
- Corrected blob offsets in `Pile` so retrieved blobs no longer include headers or
  branch records.
- Scheduled branch writes through the pile's write handle to avoid orphaned
  branch heads when crashes occur before pending blobs flush.
- Applied branch head updates immediately and sized branch records using
  `size_of` to preserve compare-and-swap semantics without magic numbers.
- Fixed compiler warnings by clarifying lifetime elision and ignoring
  generated imports when unused.
- Removed remaining 64-byte assumptions from blob writes by computing header
  length and padding with `size_of::<BlobHeader>()`.
- `ignore!` now hides variables correctly by subtracting them from inner constraints.
- ByteTable resize benchmark now reports load factor for fully populated 256-slot tables.
- `PatchIdConstraint` incorrectly used 32-byte values when confirming IDs, causing
  `local_ids` queries to return no results with overridden estimates.
- Documentation proposal for exposing blob metadata through the `Pile` API.
- Branch updates now sync branch headers to disk to avoid losing branch pointers after crashes.
- `IndexEntry` now stores a timestamp for each blob. `PileReader::metadata`
  returns this timestamp along with the blob length.
- Design notes for a conservative garbage collection mechanism that scans
  `SimpleArchive` values in place to find reachable handles.
- Clarified that accidental collisions are practically impossible given 32-byte
  hashes, explaining why the collector can treat any matching value as a real
  reference.
- Expanded the book's garbage collection chapter with clearer reachability
  description, traversal overview and handle-based pruning.
- Repository workflows chapter covering branching, merging, CLI usage and an improved push/merge diagram.
- Separate `verify.sh` script for running Kani verification.
- Documented conflict resolution loop and clarified that returned workspaces
  contain updated metadata which must be pushed.
- Explained BranchStore's CAS-based optimistic concurrency control in the
  repository guide.
- Property tests for `ufoid` randomness and timestamp rollover.
- Further clarified `timestamp_distance` documentation that it only works with
- Documentation for built-in schemas and how to create your own.
  timestamps younger than the ~50-day rollover period.
- Added `HybridStore` to combine separate blob and branch stores.
- Added tests for the `ObjectStoreRemote` repository using the in-memory
  object store backend.
- Implemented `Debug` for `ObjectStoreRemote` and replaced `panic!` calls
  with `.expect()` in object store tests.
- Initial scaffold for a narrative "Tribles Book" documentation.
- Build script `build_book.sh` and CI workflow to publish the mdBook.
- Expanded the introduction and philosophy sections of the Tribles Book and
  documented how to install `mdbook`.
- Documented the pile file format in the book and expanded it with design rationale.
- Expanded the pile format chapter with recovery notes and a link to the `Pile` API docs.
- Added a book chapter describing the `find!` query language, listed
   built-in constraints, and included a reusable sample dataset for
   documentation examples.
- Added an architecture chapter that explains how `TribleSet` differs from the repository layer and details branch stores and commit flow. The diagram now better illustrates the commit flow.
- Added a "Developing Locally" chapter and linked it from the README and book introduction.
- Expanded the architecture chapter with design goals, semantic background and
  cross-references to other chapters.
- Clarified that the branch store's compare-and-set operation is the only
  place-oriented update, leaving the rest of the system value oriented and
  immutable.
- Documented the incremental query plan in `INVENTORY.md` and linked it
  to a new "Incremental Queries" book chapter detailing the approach.
- Noted that namespaces will expose a `delta!` operator, similar to
  `pattern!`, for expressing changes between `TribleSet`s. The macro
  computes the difference and uses `union!` internally to apply the
  delta constraint.
 - Documented potential commit selector redesign using git-style
   reachability semantics. Added a "Commit Selectors" design note with
    a table comparing Git syntax to the planned set-based API. The table
    is now exhaustive for Git's revision grammar, using only the general
    forms. Each entry links to the official documentation and marks
    selectors that are not planned for the initial implementation.
- Noted plans for a `delta!` operator to assist with incremental
  queries. Documentation describes how it will union patterns with
  each triple constrained to the dataset delta.
- Recorded a future task to generate namespaces from a TribleSet
  description and to rewrite `pattern!` as a procedural macro.
- Documented the internal `pattern_inner!` macro with expanded usage notes.
- Added inline comments for every `pattern_inner!` rule describing what it
  matches and why.
- Added a "PATCH" chapter to the book's deep dive section explaining the trie
  implementation.
- Recorded tasks to benchmark PATCH, analyze its algorithmic complexity and
  measure real-world space usage.
- Listed candidate built-in schemas with design notes in `INVENTORY.md` for
  future implementation.
- Documented commit range semantics explaining that `a..b` equals
  `ancestors(b) - ancestors(a)` with missing endpoints defaulting to an empty set
  and the current `HEAD`.
- Commits now record a `timestamp` using `NsTAIInterval` and workspaces provide a
  `TimeRange` selector to gather commits between two instants.
- Compressed zero-copy archives are now complete.
- Incremental queries use a new `pattern_changes!` macro.
- Added an `exists!` macro (formerly `matches!`) mirroring `find!` for boolean checks.
- Regular path queries via a new `RegularPathConstraint` and namespaced `path!` macro.
- `path!` automata now store transitions in a `PATCH` for efficient lookups and set operations.
- Added a `filter` commit selector with a `history_of` helper.

### Changed
- Switched `anybytes` to a git dependency and used its `Bytes` integration
  to avoid copying blob data when writing to object stores.
- README no longer labels compressed zero-copy archives as WIP.
- Switched from `sucds` to `jerky` for succinct data structures and reworked
  compressed archives to use it directly.
- Construct archive prefix bit vectors using `BitVectorBuilder::from_bit`.
- Removed completed tasks from `INVENTORY.md` and recorded them here.
- Removed the experimental `delta!` macro implementation; incremental
  query support will be revisited once `pattern!` becomes a procedural
  macro.
- Split branch lookup tests into independent cases for better readability.
- `Repository::checkout` was renamed to `pull` for symmetry with `push`.
- `IntoCheckoutRange` trait became `CommitSelector` and its `into_vec` method
  was renamed to `select`.
- Updated bucket handling to advance RNG state in `bucket_shove_random_slot`.
- Clarified need for duplicate `bucket_get_slot` check in `table_get_slot`.
- Replaced Elias--Fano arrays in `SuccinctArchive` with bit vectors for
  simpler builds and equivalent query performance.
- `SuccinctArchive` now counts distinct component pairs using bitsets,
  improving query estimation accuracy.
- Domain enumeration skips empty identifiers via `select0` and prefix bit
  vectors are constructed with `BitVector` for lower memory overhead.
- Improved `Debug` output for `Query` to show search state and bindings.
- Replaced branch allocation code with `Layout::from_size_align_unchecked`.
- Removed unused `FromBlob` and `TryToBlob` traits and updated documentation.
- Documented how `MemoryBlobStore::insert` deduplicates blobs by handle in the
  deep dive chapter.
- Simplified constant comparison in query tests.
- `pattern!` now reuses attribute variables for identical field names.
- Clarified that the project's developer experience goal also includes
  providing an intuitive API for library users.
- Renamed the `delta!` macro to `pattern_changes!` and changed its
  signature to `(current, changes, [pattern])` assuming the caller
  computes the delta set.
- Documented Kani proof guidelines to avoid constants and prefer
  `kani::any()` or bounded constructors for nondeterministic inputs.
- Fixed Kani playback build errors by using `dst_len` to access `child_table`
  length without implicit autorefs.
- Introduced `InlineEncoding::validate` to verify raw value bit patterns.
- Query and value harnesses use this to avoid invalid `ShortString` data during playback.
- `InlineEncoding::validate` now returns a `Result` and `Inline::is_valid` provides
  a convenient boolean check.
- Corrected the workspace example to merge conflicts into the returned workspace
  and push that result.
- `preflight.sh` now only checks formatting and runs tests; Kani proofs run via `verify.sh`.
- Removed instruction to report unrelated Kani failures in PRs.
- Added missing documentation for several public structs and functions in
  `blob` and `repo` modules.
- Expanded the descriptions to clarify usage of public repository APIs.
- Moved repository and pile guides into module documentation and updated README links.
- Simplified toolchain setup. Scripts install `rustfmt` and `cargo-kani` via
  `cargo install` and rely on the system's default toolchain.
- Depend on the crates.io release `hifitime` 4.1.2 instead of the git repository.
- Added a README "Getting Started" section demonstrating `cargo add tribles` and
  a pile-backed repository example.
- Documented iteration order of `MemoryBlobStoreReader`, noted workspace use of
  `MemoryBlobStore::new` and improved `Pile::try_open` description.
- Restricted `PileSwap` and `PileAux` to crate visibility.
- Repository guidelines now discourage asynchronous code in favor of
  synchronous implementations that can be parallelized.
- Renamed `ObjectStoreRepo` to `ObjectStoreRemote` in the object-store backend.
- Listing iterators for the object-store backend now stream directly from the
  underlying store instead of collecting results in memory.
- `Repository::push` now returns `Option<Workspace>` instead of the custom
  `RepoPushResult` enum, simplifying conflict handling.
- Split identifier and trible structure discussions into dedicated deep-dive book chapters.
- `preflight.sh` now verifies that the mdBook documentation builds successfully.
- Fixed book `SUMMARY.md` so preflight passes without parse errors.
- `Workspace` now exposes a `put` method for adding blobs, replacing the old
  `add_blob` helper. The method returns the stored blob's handle directly since
  the underlying store cannot fail.
- `Workspace::get` method retrieves blobs from the local store and falls back to
  the base store when needed.
- `ReadError` now implements `std::error::Error` and provides clearer messages when opening piles.
- Removed the `..=` commit range selector. The `..` selector now follows Git's
  semantics and excludes the starting commit.
- Extracted `collect_range` into a standalone function for clarity.
- Moved `first_parent` into a standalone function for clarity.
- Added a `collect_reachable` helper to gather all commits reachable from a
  starting point.
- Scalar commit selectors once again return only the specified commit.
- Introduced an `ancestors` selector to retrieve a commit and its history.
- Commit selectors now return a `CommitSet` patch of commit handles instead of a `Vec`.
- Renamed the `CommitPatch` type alias to `CommitSet`.
- The `..` commit selector now walks from the end boundary until it encounters
  a commit returned by the start selector. To reproduce Git's set-difference
  semantics, wrap the boundary explicitly as `ancestors(start)..end`.
- Added a `symmetric_diff` selector corresponding to Git's `A...B` three-dot
  syntax.
- Refined candidate built-in schemas in `INVENTORY.md`; removed `Bool`, the
  `BinaryLargeObject` placeholder, and the 64-bit integer types.
- Expanded the built-in schema ideas with a fuller list of value and blob
  formats to explore.
- Brainstormed an even broader range of potential schemas for long-term
  consideration.
- Added Lance, neural-network, vector-search and full-text index formats to the
  candidate blob schemas, with a note to favor memory-mapped Rust crates.
- Trimmed the candidate schemas, dropping seldom-used formats like neural
  networks, search indexes, media and font types.
- Reinstated the neural-network, HNSW and full-text index schema ideas and
  removed the tar/zip archive formats.
- Added `SocketAddr` and `RgbaColor` value types alongside a `CompressedBlob`
  wrapper, while dropping `DateYMD` and `TimeOfDay` from consideration.
- `RangeFrom` now returns `ancestors(head)` minus `ancestors(start)` while
  `..c` selects `ancestors(c)` and `..` resolves to `ancestors(head)`. The old
  `collect_range` and `first_parent` helpers were removed.
- `TimeRange` commit selector now delegates to the generic `filter` selector.
- Removed the `Completed Work` section from `INVENTORY.md`; finished tasks are
  now tracked in this changelog.
- Canonicalized epsilon closures in regular path queries and documented the
  Thompson-style automaton construction.
- Documented the currently implemented commit selectors in the book.

### Fixed
- Enforce `PREFIX_LEN <= KEY_LEN` for prefix checks in PATCH.
- Release file locks if `refresh` fails during pile branch updates to avoid lingering locks.
- Blob insertion now returns an error instead of panicking if the system clock goes backwards.
- Delay branch map updates until after branch records are written to disk, preventing divergence when writes fail.

## [0.5.2] - 2025-06-30
### Added
- Initial changelog file.
- Repository guidelines now require documenting tasks in `CHANGELOG.md`.
- Converted object-store backend to `BranchStore`/`BlobStore` API.
