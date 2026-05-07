# Distributed Sync

The [`triblespace-net`](https://github.com/triblespace/triblespace-rs/tree/main/triblespace-net)
crate adds peer-to-peer synchronization over [iroh](https://www.iroh.computer/):
gossip for HEAD announcements, a DHT for content discovery, direct QUIC
for bulk transfer. The user-visible surface is a single wrapper type —
`Peer<S>` — that makes any triblespace store also a node on a
distributed graph, without changing how the storage traits look from
outside.

Enable it through the facade crate's `net` feature:

```toml
[dependencies]
triblespace = { version = "x.y.z", features = ["net"] }
```

```rust,ignore
use triblespace::net::peer::{Peer, PeerConfig};
```

## Mental Model

`Peer<S>` takes any `S: BlobStore + BlobStorePut + BranchStore<Blake3>`
and wraps it into a node that participates in the iroh network. Two
layers of behavior are bolted onto the normal storage trait calls:

- **Reads auto-drain incoming gossip.** Every call through `reader()`,
  `head(id)`, or `branches()` transparently pulls any pending
  `NetEvent`s from the network thread into the wrapped store and
  re-publishes any deltas from external writers (e.g. another process
  appended to the same pile file). Mirrors `Pile::refresh` — the
  explicit `Peer::refresh` method is available for tight loops, but
  normal storage use Just Works.
- **Writes auto-publish.** Calls through `put` / `update` delegate to
  the inner store and then announce blobs to the DHT and gossip branch
  HEADs to the topic mesh, all via the background network thread.

The network thread is a private implementation detail: `Peer::new`
spawns it; `Peer::drop` winds it down. Async stays jailed inside that
thread — the storage traits stay sync.

```rust,ignore
use std::collections::HashSet;

let pile = triblespace::core::repo::pile::Pile::open(path)?;
let peer = Peer::new(pile, signing_key.clone(), PeerConfig {
    peers: vec![bootstrap_endpoint_id],
    gossip: true,                            // false = pull/serve-only
    // Auth is mandatory — see the Capability Auth chapter for the
    // team-root + self_cap setup, or run `trible team create`.
    // The team root pubkey doubles as the gossip mesh id when
    // `gossip = true`.
    team_root: signing_key.verifying_key(),  // single-user team-of-one
    revoked: HashSet::new(),
    self_cap: [0u8; 32],
});
let mut repo = Repository::new(peer, signing_key, TribleSet::new())?;
// From here it's just a Repository — commit, push, pull, query.
```

## Tracking Branches

When a peer learns about a remote HEAD — via gossip arrival or an
explicit `track` call — it materializes the data as a **tracking
branch**: a local branch whose metadata carries `tracking_remote_branch`
(the remote branch id), `tracking_peer` (the publisher's key), and
`remote_name` (instead of the usual `metadata::name`). This keeps
tracking branches invisible to normal discovery: `ensure_branch(name)`
won't find them, `lookup_branch(name)` returns only your own branches,
and the `is_tracking_branch` filter lets the Peer avoid re-gossiping
its mirrors back to the network.

Tracking branches are your sandbox for remote state. Merging them into
your own same-named branch is how you "accept" the remote changes (see
the *Merge Flow* section below).

## Transports

Three protocols ride on the same iroh endpoint:

- **Gossip mesh** (HyParView + PlumTree via `iroh-gossip`): all peers
  on the same topic receive every branch HEAD announcement. 81-byte
  messages: a 1-byte tag, 16-byte branch id, 32-byte HEAD hash,
  32-byte publisher key. Eventual delivery; duplicates deduped on
  the wire.
- **DHT** (via `iroh-dht`): content discovery for blobs. On write,
  `announce_provider(blob_hash)` tells the DHT "I have this blob." On
  read, `find_providers(blob_hash)` returns peers to fetch from.
  Content-addressed by design — any provider with the right bytes
  passes blake3 verification.
- **Direct QUIC RPC** (`PILE_SYNC_ALPN = "/triblespace/pile-sync/4"`):
  point-to-point operations that don't fit the gossip model —
  listing a peer's branches, asking for a specific branch's HEAD,
  fetching a single blob by hash, enumerating a blob's child
  references. One stream per operation, stream FIN signals end, nil
  sentinels (zero branch ids / zero hashes) terminate sequences. The
  protocol's first stream on every connection must be `OP_AUTH` —
  see the [Capability Auth](capability-auth.md) chapter for the
  full handshake and scope-gating semantics.

## `track` vs `fetch`

Two primitives cover the two levels of "go get this":

- `peer.track(endpoint_id, branch_id)` — fire-and-forget. Opens a
  QUIC stream to the remote, asks for its HEAD, then walks the
  reachable closure of blobs (BFS over the parent-to-children graph
  via `op_children`, pulling each blob through DHT-first then
  peer-fallback). When the whole closure has landed locally, emits a
  `NetEvent::Head` that the Peer drains into a freshly-materialized
  tracking branch. The tracking branch only advances **after** every
  referenced blob is in the pile — external readers either see the
  old HEAD (with its complete closure) or the new HEAD (with its
  closure), never a half-torn state.
- `peer.fetch::<T, Sch>(endpoint_id, handle)` — blocking single-blob
  RPC. Pass a typed handle, pick what comes out: `Blob<Sch>` for
  bytes-only with zero decode cost, or the decoded type (`TribleSet`,
  `anybytes::View<str>`, etc.) for the deserialized value. The bytes
  land in the wrapped store via `BlobStorePut::put` and the return
  value is decoded from those same bytes.

For the common "pull a branch by name" workflow, `peer.pull_branch(
endpoint_id, name)` composes them: list the remote's branches, pull
each metadata blob via `fetch`, query for `metadata::name`, find the
match, hand off to `track`, block until the tracking branch
materializes. Returns the local tracking branch id ready to merge.

## Merge Flow

Once a tracking branch exists, merging it into its same-named local
branch is the normal Repository workflow plus one helper:

```rust,ignore
use triblespace::net::tracking::{merge_tracking_into_local, MergeOutcome};

match merge_tracking_into_local(&mut repo, tracking_id, "main")? {
    MergeOutcome::Empty      => { /* tracking had no head yet */ }
    MergeOutcome::UpToDate   => { /* local already at that state */ }
    MergeOutcome::Merged { new_head } => {
        // local "main" advanced — either fast-forward or a real
        // merge commit, decided by Workspace::merge_commit.
    }
}
```

Under the hood that's `ensure_branch("main")` + `pull` tracking
workspace + `pull` local workspace + `merge_commit(remote_head)` +
conditional `push`. The `merge_commit` call picks no-op /
fast-forward / merge commit based on ancestor-walking.

For long-running sync daemons, the same helper runs in a loop over
every tracking branch on every refresh tick.

**Convergence rounds.** When two peers diverge on the same branch:

- *Sequential gossip* (one peer's merge lands before the other's starts)
  converges in one round-pair. The first side produces a merge commit
  `AM` containing both original commits as parents; the second side
  sees `AM`, finds its own head in `ancestors(AM)`, and fast-forwards.
- *Parallel gossip* (both peers merge before either sees the other's
  merge) also converges in one round-pair — and without producing a
  merge commit on the second side. Merge commits are **content-addressed**:
  they carry no author-specific bits (no signature, no `created_at`,
  entity id derived intrinsically from the parent set via `entity!`'s
  content-hash form), so two peers merging the same parent set produce
  bit-identical merge commits that dedup via blob hash alone.

Either way the system converges in one round-pair. The tests in
`triblespace-net/tests/two_peer_convergence.rs` exercise both cases
and serve as regression coverage for the property. Content-addressed
merges are also why `merge_tracking_into_local` is safe to run in a
tight polling loop without worrying about merge-commit churn.

## Ordering Under Pressure

Gossip is eventually consistent, which means a flood of HEAD updates
can arrive out of order: HEAD_1 → HEAD_2 → HEAD_3 where HEAD_1's
closure happens to take longer over the DHT and completes *after*
HEAD_3 has already advanced the tracking branch. Without protection,
HEAD_1 would clobber HEAD_3 and the branch would regress.

To prevent this, `branch_metadata` stamps every published branch
metadata blob with `metadata::updated_at: NsTAIInterval` from
`Epoch::now()`. TAI is strictly monotone (no leap-second jumps).
`update_tracking_branch` reads the stamp from both the current and
incoming metadata and rejects updates whose timestamp is not strictly
newer — logged as `[tracking] skip stale update for branch <bid>`
for observability. The synthesized tracking branch metadata mirrors
the remote's timestamp so subsequent comparisons share a reference
frame.

Tradeoff: publishing the same HEAD twice at different moments produces
different metadata blob hashes now (the timestamps differ). Gossip
convergence degrades slightly — duplicate blobs for the same semantic
state — but correctness is preserved and regressions are eliminated.

## CLI Surface

The `trible` CLI exposes sync via the `pile net` subcommand:

```
trible pile net identity [--key PATH]
    Print this node's iroh identity (generates a key if needed).

trible pile net sync <PILE> [--peers ...] [--key PATH]
    Long-running bidirectional sync on the team's gossip mesh.
    The mesh is identified by the team root pubkey directly (no
    separate --topic flag): every team has exactly one mesh,
    derived from its identity. Auto-merges incoming tracking
    branches into same-named local ones every tick. Reads
    `TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP` env vars for multi-
    user team operation; falls back to single-user team-of-one
    using the node's own pubkey when those aren't set.

trible pile net pull <PILE> <REMOTE> --branch NAME [--key PATH]
    One-shot pull of a named branch from a specific peer (REMOTE is
    the peer's iroh node id, 64-char hex). Pull-only mode — no gossip
    subscription, direct QUIC + DHT fetch, materialize a tracking
    branch, merge into local. Useful for "give me a copy of that
    project" workflows. Same env-var fallback as `sync`.

trible team {create, invite, revoke, list}
    Team capability lifecycle — see the Capability Auth chapter.
```

## What's Deferred

A few structural improvements the design discussion has surfaced but
that aren't implemented yet:

- **Incremental commit-chain advance.** Today the tracking branch only
  moves when the whole reachable closure of a HEAD is local. Under
  sustained gossip pressure on large histories, we could fall
  arbitrarily behind. A git-like incremental walker (parallel-fetch
  commit contents, advance the tracking branch commit-by-commit in
  topological order) would give steady progress at the cost of
  exposing intermediate states to readers.
- **`CachingStore<P>` with on-miss fetch.** A middleware that wraps a
  `Peer` and does DHT-backed on-miss fetching inside `BlobStoreGet::get`,
  with a policy callback for gating by size / schema / context. Would
  cover the "cache eviction + lazy fetch" workflows that current
  eager-only semantics can't.
- **Schema-aware traversal in `track`.** `op_children` today scans
  parent blob bytes for 32-byte chunks that look like hashes. That's
  cheap and peer-agnostic but pulls more than strictly necessary when
  a blob contains handle-sized non-hash data. A schema-aware walker
  that parses each blob as its declared schema and enumerates referenced
  handles could be precise, but adds significant traversal complexity.

All three are additive: the current model stays correct as a
strict-closure / eager-only baseline that these improvements build on.
