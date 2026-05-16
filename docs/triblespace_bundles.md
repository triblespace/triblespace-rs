# TribleSpace bundles (Fragments + blobs)

`Fragment` gives us an ergonomic, unified abstraction for "a slice of graph plus
exported entrypoints". The missing piece is: how do we move/persist a fragment
*together with* any blobs it references (attachments, LongStrings, archives,
etc.)?

The naive answer is "make a type that contains `Fragment` + blob bytes", but
that quickly becomes an anti-pattern: blobs can be large and holding them in
memory duplicates what `Workspace` already does (staging).

This document proposes a different reframing: treat "graph + blobs" as a **blob
reachability problem**, and make bundling a **handle-manifest problem** (roots,
not payloads).

## Existing primitives

- Facts are persisted as blobs:
  - `Workspace::commit(content, ...)` stores `content` as a `SimpleArchive`
    blob and stores a commit-metadata blob that references it.
  - `Workspace::put(item)` stages arbitrary blobs in `workspace.local_blobs`.
- Pushing a workspace uploads staged blobs and CAS-updates the branch head:
  - `Repository::try_push` copies `workspace.local_blobs` into repo storage.
- We already have generic blob-graph tooling:
  - `repo::reachable(source, roots)` performs a BFS over blob handles.
  - `repo::transfer(handles, source, target)` copies blobs by handle.
  - `repo::potential_handles(&TribleSet)` scans the value column for candidate
    `Handle<..>` values (useful when you *have* a `TribleSet`).

## Key idea: bundles are *roots*, not bytes

A "bundle" can be represented as a small set of **root handles**. Given roots,
we can compute the transitive closure of blobs needed to make those roots
dereferenceable in another store, then `transfer` that closure.

In practice, a **commit handle** is already an excellent bundle root:

- A commit blob references:
  - the content blob (`SimpleArchive`), and optionally metadata and message
    blobs.
- The content blob references:
  - any additional blobs via typed handle values (e.g. `LongString`, `RawBytes`,
    `WasmCode`, ...).

So "exporting a graph" becomes:

1. Commit it (`Workspace::commit(fragment, ...)` drops `Fragment` exports and
   persists only facts).
2. Use the resulting commit handle as the bundle root.
3. Transfer `reachable(storage, [commit_handle])` into the target store.

This avoids inventing a new "facts+blobs" container type and keeps blob payloads
streamable / store-backed.

## What about `Fragment` exports?

`Fragment` exports are intentionally *out-of-band*: they're a producer-friendly
interface, not a privileged part of the graph model.

If exports need to be persisted across process boundaries, persist them as
normal facts (domain-specific), e.g.:

- An "import result" entity that points at exported entity id(s).
- A "workspace snapshot" entity that records root(s) for navigation.

Bundles can optionally carry exported ids as *metadata*, but that should be a
separate concept from "the closure of blobs needed for dereferencing".

## Proposed API shape (lightweight manifest)

We can add a small utility type without ever storing blob bytes:

```rust
pub struct Bundle<H: HashProtocol> {
    // Deterministic set semantics (like Fragment exports).
    roots: PATCH<32>,
    _phantom: PhantomData<H>,
}
```

Where `Bundle` is just ergonomic sugar around:

- `reachable(source, bundle.roots())`
- `transfer(reachable(...), source, target)`

Benefits:
- Bundles are cheap to clone, compare, and persist (as data) if desired.
- Bundles are independent of staging strategy (memory, disk, remote).

Non-goals:
- Do not embed blob payloads.
- Do not attempt to define a universal "schema" for what a bundle *means*.

## Reachability scanning strategy (open question)

Today `reachable` scans each blob's bytes for 32-byte windows and probes the
store to see if they are handles. This is maximally generic, but it has two
downsides:

- Performance: scanning huge blobs (e.g. `RawBytes`) is expensive and can
  trigger many remote `get` probes.
- Semantics: opaque file bytes should generally *not* be interpreted as a graph
  of references.

A more semantics-aligned approach for TribleSpace repos is:

- Try decoding a blob as `SimpleArchive -> TribleSet`.
- If it decodes, derive next handles via `potential_handles(&set)`.
- Otherwise, treat the blob as a leaf.

This gives us "reachability for TribleSets/commits" while avoiding accidental
dependency discovery inside arbitrary bytes.

We can implement this as either:
- a new `reachable_archives(...)`, leaving `reachable(...)` as the generic
  byte-scanning fallback, or
- configurable reachability with a scan policy (size thresholds, decoders).

## Large blob staging (separate concern)

Even with bundle roots, writing large blobs still needs a good staging story.
Right now `Workspace::put` stages in-memory (`MemoryBlobStore`). For large blobs
this can be undesirable.

This is orthogonal to bundling and can be addressed with:
- disk-backed staging, or
- direct-to-base-store puts for large payloads (with explicit durability
  semantics).

## Next steps

1. Decide whether we want an explicit `Bundle` type (or keep bundles as plain
   `Vec<Handle<UnknownBlob>>` roots).
2. Decide on reachability policy for `RawBytes` and other opaque blobs.
3. Add a small cookbook section to the Tribles book showing:
   - "commit a fragment, export by commit handle"
   - "transfer reachable closure to a new repo"

