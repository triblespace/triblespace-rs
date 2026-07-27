# Architecture Overview

TribleSpace is designed to keep data management simple, safe and fast.  The [README](../README.md) introduces these goals in more detail, emphasizing a lean design with predictable performance and straightforward developer experience.  This chapter explains how the pieces fit together and why they are organised this way.

## Design Goals

A full discussion of the motivation behind TribleSpace can be found in the [Philosophy](deep-dive/philosophy.md) section.  At a high level we want a self‑contained data store that offers:

- **Simplicity** – minimal moving parts and predictable behaviour.
- **Developer Experience** – a clear API that avoids complex servers or background processes.
- **Safety and Performance** – sound data structures backed by efficient content addressed blobs.

These goals grew out of earlier "semantic" technologies that attempted to model knowledge as graphs.  While systems like RDF promised great flexibility, in practice they often became difficult to host, query and synchronise.  TribleSpace keeps the idea of describing the world with simple statements but stores them in a form that is easy to exchange and reason about.

## Design Principles

Three load-bearing decisions shape everything else in TribleSpace.  Understanding them up front makes the rest of the architecture — the six indexes, the append-only storage, the branch/commit model, the absence of `delete` — read as consequences rather than costs.

### 1. Content Addressing

Every blob is identified by the hash of its bytes.  Identical data deduplicates automatically, integrity is verifiable offline, and repositories can share data through any common storage without coordination.  Handles are 32-byte hashes, which means they fit inline in a trible's value slot: a value either *is* its data (for short payloads) or *points to* its data (via a blob hash).  This is what lets TribleSpace be "content-addressed all the way down" — schemas, commits, branch metadata, and application data all use the same primitive.

### 2. Monotonic Facts

Tribles are only added, never retracted.  A `TribleSet` is a mathematical set of facts, and merging two sets is simply set union.  There is no `delete` operation and no "latest wins" heuristic inside the data model.  This follows the [CALM principle](https://arxiv.org/abs/1901.01930): monotonic operations are coordination-free, so distributed replicas can merge without consensus.  It is also what makes `TribleSet` a CRDT — two workspaces can edit independently and always reconcile cleanly.

The apparent limitation (how do you model mutable state?) is resolved by the next principle.

### 3. Entity Ownership

This is the decision that distinguishes TribleSpace from other triple stores.  In RDF and similar systems, triple direction has no semantics — `parentOf` and `childOf` are interchangeable and systems typically auto-infer one from the other.  TribleSpace gives direction **provenance semantics**: a trible `A → attribute → B` is always a claim made *by* A *about* B, and only the current owner of an entity ID may assert new facts with that entity in the subject position.

This ownership discipline is enforced through [`ExclusiveId`](https://docs.rs/triblespace/latest/triblespace/id/struct.ExclusiveId.html) guards, which are `Send` but not `Sync` — holding one guarantees that no other process is writing about that entity.  In other words, **each entity forms its own transactional shard**.  You can think of it as Rust's ownership model applied to data: just as the borrow checker prevents two threads from mutating the same variable, the ID ownership system prevents two processes from asserting conflicting attributes about the same entity.

The consequences are profound:

- **Merges cannot conflict by construction.** Two workspaces that edit different entities can always merge, because neither can have written about the other's entities.  The "merge conflict resolution" problem that plagues distributed databases simply doesn't exist in this model.
- **Non-monotonic operations become safe within an ownership scope.** While the global data model stays monotonic, an owner holding a set of `ExclusiveId`s has a closed-world view of those entities.  Operations like `if-does-not-exist` are well-defined within that transaction domain because no other writer can intervene.
- **Mutable state is modelled as ownership + replacement.** To "update" an entity's attribute, you mint a new entity and reference it from the owner.  The old fact remains in the history; the current view is determined by what the owner currently points to.  This is the same pattern as immutable data structures: mutation becomes a new version, and the "current" value is a pointer that gets swapped.

Entity ownership handles per-entity consistency, but some workflows need stronger guarantees — transactions that span multiple entities, or invariants like "these two facts must be visible atomically."  For those cases the branch store's compare-and-set update provides defense in depth: a workspace stages its changes locally, and `push` only succeeds if the branch head hasn't moved since the workspace was pulled.  On conflict, the caller merges the incoming changes and retries.  This gives you serializable multi-entity transactions on top of the monotonic data model, at the cost of a retry loop under contention.

Together the two layers cover the full transaction story: entity ownership for "I own this thing, let me just update it," and branch CAS for "these N things must move as one."  Neither requires the user to think about distributed coordination protocols.

The ID ownership system is documented in depth in [Identifiers](deep-dive/identifiers.md); the rest of this chapter assumes these three principles as given.

## Architectural Layers

The system is organised into a small set of layers that compose cleanly:

```text
┌─────────────────────────────────────────────┐
│  Application                                │
│  find!, pattern!, entity!            │
├─────────────────────────────────────────────┤
│  Workspace                                  │
│  in-memory editing surface, blob read/write │
├─────────────────────────────────────────────┤
│  Repository                                 │
│  branches, commits, push/pull, merge        │
├─────────────────────────────────────────────┤
│  Store (Pile / MemoryRepo / S3)             │
│  append-only blob + branch storage          │
├─────────────────────────────────────────────┤
│  Data Model                                 │
│  Trible (64 bytes) → TribleSet (6 indexes)  │
└─────────────────────────────────────────────┘
```

1. **Data model** – the immutable trible structures that encode facts.
2. **Stores** – generic blob and branch storage traits that abstract over persistence backends.
3. **Repository** – the coordination layer that combines stores into a versioned history.
4. **Workspaces** – the in‑memory editing surface used by applications and tools.

Each layer has a tight, well defined boundary.  Code that manipulates tribles never needs to know if bytes ultimately land on disk or in memory, and repository level operations never reach inside the data model.  This separation keeps interfaces small, allows incremental optimisation and makes it easy to swap pieces during experimentation.

## Data Model

The fundamental unit of information is a [`Trible`](https://docs.rs/triblespace/latest/triblespace/trible/struct.Trible.html).  Its 64 byte layout is described in [Trible Structure](deep-dive/trible-structure.md).  A `Trible` links a subject entity to an attribute and value.  Multiple tribles are stored in a [`TribleSet`](https://docs.rs/triblespace/latest/triblespace/trible/struct.TribleSet.html), which behaves like a hashmap with three columns — subject, attribute and value.

The 64 byte boundary allows tribles to live comfortably on cache lines and makes deduplication trivial.  Because tribles are immutable, the runtime can copy, hash and serialise them without coordinating with other threads.  Higher level features like schema checking and query planning are therefore free to assume that every fact they observe is stable for the lifetime of a query.

## Trible Sets

`TribleSet`s provide fast querying and cheap copy‑on‑write semantics.  They can be merged, diffed and searched entirely in memory.  When durability is needed the set is serialised into a blob and tracked by the repository layer.

To keep joins skew‑resistant, each set maintains all six orderings of entity,
attribute and value.  The trees reuse the same leaf nodes so a trible is stored
only once, avoiding a naïve six‑fold memory cost while still letting the search
loop pick the most selective permutation using the constraint heuristics.

## Blob Storage

All persistent data lives in a [`BlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/index.html).  Each blob is addressed by the hash of its contents, so identical data occupies space only once and readers can verify integrity by recomputing the hash.  The trait exposes simple `get` and `put` operations, leaving caching and eviction strategies to the backend.  Implementations decide where bytes reside: an in‑memory [`MemoryBlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/struct.MemoryBlobStore.html), an on‑disk [`Pile`](https://docs.rs/triblespace/latest/triblespace/repo/pile/struct.Pile.html) described in [Pile Format](pile-format.md) or a remote object store.  Because handles are just 32‑byte hashes, repositories can copy or cache blobs without coordination.  Trible sets, user blobs and commit records all share this mechanism.

Content addressing also means that blob stores can be layered.  Applications commonly use a fast local cache backed by a slower durable store.  Only the outermost layer needs to implement eviction; inner layers simply re-use the same hash keys, so cache misses fall through cleanly.

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

The boundaries between layers encourage modular tooling.  A CLI client can operate entirely within a workspace while a sync service automates pushes and pulls between repositories.  As long as components honour the blob and branch store contracts they can evolve independently without risking the core guarantees of TribleSpace.
