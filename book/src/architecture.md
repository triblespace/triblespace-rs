# Architecture Overview

Trible Space is designed to keep data management simple, safe and fast.  The [README](../README.md) introduces these goals in more detail, emphasizing a lean design with predictable performance and straightforward developer experience.  This chapter explains how the pieces fit together and why they are organised this way.

## Design Goals

The architecture keeps four values in view: clarity before cleverness, a productive synchronous API surface, strong validation at the boundaries, and predictable performance. These grew out of earlier "semantic" technologies that attempted to model knowledge as graphs.  While systems like RDF promised great flexibility, in practice they often became difficult to host, query and synchronise.  Trible Space keeps the idea of describing the world with simple statements but stores them in a form that is easy to exchange and reason about.

## Identifier taxonomy

Identifiers fall into two orthogonal axes that surface throughout the storage and repository layers:

|                | **Abstract**                    | **Semantic**                |
|----------------|---------------------------------|-----------------------------|
| **Intrinsic**  | Content hashes, signatures      | Embeddings                  |
| **Extrinsic**  | RNGID, UFOID, FUCID, namespaced | Names, URLs, DOIs, labels   |

Abstract identifiers prioritise uniqueness; semantic ones carry meaning for people or search systems. Intrinsic identifiers are derived from the bytes they describe (for example 256‑bit hashes), while extrinsic identifiers are assigned externally and persist even as content evolves. Trible Space leans on abstract extrinsic identifiers for entities and attributes so writers can mint stable handles without coordination, and pairs those with intrinsic identifiers when a value references immutable content such as a blob. The high‑entropy families (`RNGID`, `UFOID`, `FUCID`) keep collision risk negligible, while 256‑bit hashes remain suitable even under future cryptographic pressure.

## Architectural Layers

The system is organised into a small set of layers that compose cleanly:

1. **Data model** – the immutable trible structures that encode facts.
2. **Stores** – generic blob and branch storage traits that abstract over persistence backends.
3. **Repository** – the coordination layer that combines stores into a versioned history.
4. **Workspaces** – the in‑memory editing surface used by applications and tools.

Each layer has a tight, well defined boundary.  Code that manipulates tribles never needs to know if bytes ultimately land on disk or in memory, and repository level operations never reach inside the data model.  This separation keeps interfaces small, allows incremental optimisation and makes it easy to swap pieces during experimentation.

## Data Model

The fundamental unit of information is a [`Trible`](https://docs.rs/triblespace/latest/triblespace/trible/struct.Trible.html).
Its fixed 64‑byte layout fits cleanly in caches:

``` text
┌────────────────────────────64 byte───────────────────────────┐
┌──────────────┐┌──────────────┐┌──────────────────────────────┐
│  entity-id   ││ attribute-id ││        inlined value         │
└──────────────┘└──────────────┘└──────────────────────────────┘
└────16 byte───┘└────16 byte───┘└────────────32 byte───────────┘
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶
```

Entity and attribute identifiers are abstract, extrinsic handles. The value slot
stores either a compact payload or an intrinsic identifier (typically a
schema-qualified hash) pointing to a blob when data grows beyond 256 bits. This
rigid layout keeps serialisation trivial and makes it easy to estimate physical
size as a function of fact count. Multiple tribles are stored in a
[`TribleSet`](https://docs.rs/triblespace/latest/triblespace/trible/struct.TribleSet.html),
which behaves like a hashmap with three columns — subject, attribute and value.

## Trible Sets

`TribleSet`s provide fast querying and cheap copy‑on‑write semantics.  They can be merged, diffed and searched entirely in memory.  When durability is needed the set is serialised into a blob and tracked by the repository layer.

To keep joins skew‑resistant, each set maintains all six orderings of entity,
attribute and value.  The trees reuse the same leaf nodes so a trible is stored
only once, avoiding a naïve six‑fold memory cost while still letting the search
loop pick the most selective permutation using the constraint heuristics.

Edges point from the entity making a claim to the entity being described, which
avoids arbitrary direction choices and keeps ownership clear. Set operations
such as `union`, `intersection`, and `difference` respect the immutable nature
of facts: new information is added monotonically while subtraction is used for
snapshot comparisons rather than destructive deletion.

Identifier ownership enforces this directionality in code. `ExclusiveId`s tie an
ID to a single writer (usually a thread-local `IdOwner`), and the query helper
`local_ids` only enumerates identifiers owned by the current thread. Borrowing
an ID temporarily transfers exclusive access for updates while copy-on-write set
operations keep other readers isolated.

## Blob Storage

All persistent data lives in a [`BlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/index.html). Each blob is addressed by the hash of its contents, so identical payloads deduplicate automatically and readers can verify integrity by recomputing the hash. A blob handle couples the digest with its `BlobSchema`, keeping decoding deterministic even across language boundaries. The trait exposes simple `get` and `put` operations while backends decide where bytes reside: an in‑memory [`MemoryBlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/struct.MemoryBlobStore.html), an on‑disk [`Pile`](https://docs.rs/triblespace/latest/triblespace/repo/pile/struct.Pile.html) described in [Pile Format](pile-format.md) or a remote object store. Because handles are just 32‑byte hashes, repositories can copy or cache blobs without coordination and layer stores—for example a fast cache backed by a durable remote bucket. The same mechanism stores user payloads, archived `TribleSet`s, and commit records.

## Branch Store

A [`BranchStore`](https://docs.rs/triblespace/latest/triblespace/repo/trait.BranchStore.html) keeps track of the tips of each branch.  Updates use a simple compare‑and‑set operation so concurrent writers detect conflicts.  Both the in‑memory and pile repositories implement this trait.

Branch stores are intentionally dumb.  They neither understand commits nor the shape of the working tree.  Instead they focus on a single atomic pointer per branch.  This reduces the surface area for race conditions and keeps multi‑writer deployments predictable even on eventually consistent filesystems.

Because only this single operation mutates repository state, nearly all other logic is value oriented and immutable.  Conflicts surface only at the branch store update step, which simplifies concurrent use and reasoning about changes.

## Repository

The [`Repository`](https://docs.rs/triblespace/latest/triblespace/repo/struct.Repository.html) combines a blob store with a branch store.  Commits store a trible set blob along with a parent link and signature.  Because everything is content addressed, multiple repositories can share blobs or synchronize through a basic file copy.

Repository logic performs a few critical duties:

- **Validation** – ensure referenced blobs exist and signatures line up with the claimed authorship.
- **Blob synchronization** – upload staged data through the content-addressed blob store, which skips
  already-present bytes and reports integrity errors.
- **History traversal** – provide iterators that let clients walk parent chains efficiently.

All of these operations rely only on hashes and immutable blobs, so repositories can be mirrored easily and verified offline.

## Workspaces

A [`Workspace`](https://docs.rs/triblespace/latest/triblespace/repo/struct.Workspace.html) represents mutable state during editing.  Checking out or branching yields a workspace backed by a fresh `MemoryBlobStore`.  Commits are created locally and only become visible to others when pushed, as described in [Repository Workflows](repository-workflows.md).

Workspaces behave like sandboxes.  They host application caches, pending trible sets and user blobs.  Because they speak the same blob language as repositories, synchronisation is just a matter of copying hashes from the workspace store into the shared store once a commit is finalised.

## Commits and History

`TribleSet`s written to blobs form immutable commits.  Each commit references its parent, creating an append‑only chain signed by the author.  This is the durable history shared between repositories.

Because commits are immutable, rollback and branching are cheap.  Diverging histories can coexist until a user merges them by applying query language operations over the underlying trible sets.  The repository simply tracks which commit each branch tip points to.

## Putting It Together

```text
+-----------------------------------------------+
|                   Repository                  |
|   BlobStore (content addressed)               |
|   BranchStore (compare-and-set head)          |
+----------------------------+------------------+
           ^ push/try_push        pull
           |                         |
           |                         v
+----------------------------+------------------+
|                   Workspace                   |
|   base_blobs reader (view of repo blobs)      |
|   MemoryBlobStore (staged blobs)              |
|   current head (latest commit reference)      |
+----------------------------+------------------+
        ^             ^             |
        |             |         checkout
     commit       add_blob          |
        |             |             v
+----------------------------+------------------+
|                 Application                  |
+----------------------------------------------+
```

`Repository::pull` reads the branch metadata, loads the referenced commit, and couples that history with a fresh `MemoryBlobStore` staged area plus a reader for the repository's existing blobs.【F:src/repo.rs†L820-L848】  Workspace methods then stage edits locally: `Workspace::put` (the helper that adds blobs) writes application data into the in-memory store while `Workspace::commit` converts the new `TribleSet` into blobs and advances the current head pointer.【F:src/repo.rs†L1514-L1568】  Applications hydrate their views with `Workspace::checkout`, which gathers the selected commits and returns the assembled trible set to the caller.【F:src/repo.rs†L1681-L1697】  When the changes are ready to publish, `Repository::try_push` enumerates the staged blobs, uploads them into the repository blob store, creates updated branch metadata, and performs the compare-and-set branch update before clearing the staging area.【F:src/repo.rs†L881-L1014】  Because every blob is addressed by its hash, repositories can safely share data through any common storage without coordination.

The boundaries between layers encourage modular tooling.  A CLI client can operate entirely within a workspace while a sync service automates pushes and pulls between repositories.  As long as components honour the blob and branch store contracts they can evolve independently without risking the core guarantees of Trible Space.

## Deepen this topic

- The [Introduction](introduction.md#design-principles) distils the design
  values that shaped these layers.
- [Schemas](schemas.md#identifier-shapes-and-entropy) expands on the identifier
  taxonomy and why high-entropy IDs pair well with blob handles.
- [Query Engine](query-engine.md#search-loop) and
  [Atreides Join](atreides-join.md) show how the six-way indexes feed the
  constraint-guided search.
