# TribleSpace staging + durability tiers (closure-based sync)

This document explores a more generic and flexible model for staging and
replication in TribleSpace.

The motivating question is not "how do we bundle a `Fragment` with blobs", but:

> How do blobs get staged ergonomically and then synced between durability
> levels via a generic `reachable/transfer` mechanism, without relying on the
> caller to explicitly stage exactly the right set of blobs?

## Background: what we have today

Today, a `Workspace` is an overlay on top of a durable blob store:

- Writes go into `workspace.local_blobs` (currently `MemoryBlobStore`).
- Reads prefer local, then fall back to the base store (`Workspace::get`).
- `Repository::try_push` uploads **every** blob staged in `workspace.local_blobs`
  into the durable store, then CAS-updates branch metadata.

This is intentionally Git-shaped: a workspace feels like an index/staging area.

### Where it falls short

1. **Correctness depends on staging discipline.**
   If a commit references a blob that wasn't staged (or was staged elsewhere),
   you can publish a head that is not dereferenceable in the durable store.

2. **Large blobs make "in-memory staging" a footgun.**
   `RawBytes` / attachments can be huge; keeping payloads in RAM is often an
   anti-pattern.

3. **The Git mental model may not be buying us much.**
   The nice UX of `pull/commit/push` is real, but TribleSpace semantics are
   subtly different enough that a more universal primitive may be cleaner.

## Reframe: syncing is a reachability problem

TribleSpace already has two key primitives:

- `reachable(source, roots)`: compute the transitive closure of blob handles.
- `transfer(handles, source, target)`: copy blob payloads by handle.

If we treat the result of a write as a set of **root handles**, then making a
durable store consistent is: "transfer the reachable closure from the staging
overlay into the durable store".

This avoids "did the caller remember to stage the right blobs?" because the
closure computation is conservative: any referenced handle will be discovered
and copied.

### False positives

False positives are acceptable (and correctness is not the concern):

- A random 32-byte window colliding with a real 256-bit handle is effectively
  impossible.
- The only real downside is performance (too many membership probes / scans).

This matches other content-addressed systems: a conservative scan is correct,
and performance is a separate lever.

## Proposed model: durability tiers + overlay stores

Think of blob storage as a stack of durability tiers:

1. **Ephemeral staging** (fast, small): in-memory.
2. **Local spool** (durable-ish): disk-backed (survives process crash).
3. **Durable store** (shared): pile file or object store backend.

Each tier is a blob store; the staging view is an **overlay**:

- `get`: check highest tier first, then fall through.
- `put`: write to a chosen tier (policy-driven; e.g. spill large blobs to disk).
- `flush(roots, target)`: `transfer(reachable(overlay, roots), overlay, target)`.

This makes "moving between durability levels" a first-class concept, with a
single universal mechanism (`reachable/transfer`).

### Implication: `Workspace` becomes policy, not a data structure

`Workspace` already behaves like an overlay (local + base). The key change is:

- Stop thinking of `local_blobs` as "a set of blobs to upload".
- Start thinking of it as "an overlay tier".

Then `push` is implemented as "flush a root closure", not "upload everything in
the local tier".

## Closure-based push (sketch)

Current `try_push` shape:

1. Upload all staged blobs.
2. If head changed, create new branch-meta blob and CAS-update.

Closure-based push shape:

1. Determine the **root(s)** representing the new published state.
2. Flush reachable closure for those roots from overlay to durable store.
3. CAS-update the branch store to reference the new root blob(s).

What are the roots?

The most natural roots are **branches** viewed as CAS variables:

- A branch id is the CAS-variable identity.
- The branch store points that id at a *branch-metadata blob handle*.

So in practice the root is the **new branch-metadata handle** (which references
the new commit head and the branch name handle).

Two useful push modes likely exist:

1. **Publish branch head**: roots are `{new_branch_meta_handle}`. Flushing the
   closure from that handle is sufficient to make the published branch state
   dereferenceable (commit -> content -> referenced blobs).
2. **Flush staged (cache warmup)**: roots are "all staged blobs" (or an explicit
   attachment manifest), for workflows that want to pre-upload large blobs
   before committing references or to keep hot caches warm.

The current behavior ("upload staged blobs even if no commit is created") maps
to mode 2. If we move to closure-based push, we should make that behavior
explicit rather than accidental.

## Staging large blobs (disk spool)

To avoid the "everything in RAM" footgun, add a disk-backed staging store:

- `DiskBlobStore`: store each blob as a file keyed by handle.
- Policy: `put` into memory until `N` bytes, then spill to disk.

The overlay stack then becomes:

- Memory tier (hot)
- Disk tier (warm)
- Base tier (durable store reader)

Flush works the same: `transfer(reachable(overlay, roots), overlay, durable)`.

This keeps the API uniform while allowing large payloads to be staged safely.

## Performance knobs (optional, later)

Reachability scanning is conservative; performance depends on scanning + store
probes.

Potential future knobs:

- Size thresholds / sampling: avoid scanning huge blobs eagerly; treat
  pre-uploading as an explicit caching strategy rather than an implicit
  requirement for correctness.
- Smarter synchronization mechanisms that treat blobs as opaque:
  - Merkle-DAG style inventories,
  - bloom filters / set sketches,
  - range proofs / delta-friendly indices.

Note: TribleSpace uses 32-byte aligned handle payloads, which is compatible with
other systems that represent reference lists as dense handle arrays. Even if we
keep scanning conservative, this alignment can be leveraged for faster scans.

## What this changes (and what it doesn't)

This model does not require us to throw away `pull/commit/push`:

- Keep the ergonomic façade.
- Implement it on top of `flush(reachable(...))` so correctness doesn't depend
  on staging discipline.

It does, however, suggest making transfers a more central primitive:

- Replication between repos is just `flush` with a different target store.
- Durability upgrades (memory -> disk -> durable) are just `flush`.

## Open questions

1. Should closure-based push become the default, or an opt-in mode?
2. Do we want a first-class `flush(roots)` API on `Workspace`/`Repository`?
3. How should "pre-upload attachment blobs" be represented ergonomically:
   - explicit `flush_staged`, or
   - committing a small "attachment manifest" entity to create a root?
4. Do we want reachability policies/schema awareness now, or only once we hit
   performance cliffs?
