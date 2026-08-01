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

Every blob is identified by the hash of its bytes.  Identical data deduplicates automatically, integrity is verifiable offline, and repositories can share data through any common storage without coordination.  Handles are 32-byte hashes, which means they fit inline in a trible's value slot: a value either *is* its data (for short payloads) or *points to* its data (via a blob hash).  This is what lets TribleSpace be "content-addressed all the way down" — schemas, commits, branch names, and application data all use the same primitive.

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

Entity ownership and branch publication solve different problems. A workspace
still stages a coherent commit locally, but publishing it does not mutate a
compare-and-set branch pointer. It appends a signed assertion that an exact
branch identity contains that commit. Concurrent publications therefore remain
visible instead of racing for one scalar slot. Resolution removes only
assertions whose commits are known ancestors of another asserted commit; any
incomparable maximal commits form a frontier until later work joins them.

This keeps repository state monotonic as well as application facts. It does not
claim that independent writers receive a serializable transaction across
entities. Atomicity is the commit boundary; concurrency is represented by the
branch frontier rather than erased by a winning pointer update.

The ID ownership system is documented in depth in [Identifiers](deep-dive/identifiers.md); the rest of this chapter assumes these three principles as given.

## Architectural Layers

The system is organised into a small set of layers that compose cleanly:

```text
┌─────────────────────────────────────────────┐
│  Application                                │
│  find!, pattern!, entity!, and!, or!        │
├─────────────────────────────────────────────┤
│  Workspace                                  │
│  in-memory editing surface, blob read/write │
├─────────────────────────────────────────────┤
│  Repository                                 │
│  branches, commits, push/pull, merge        │
├─────────────────────────────────────────────┤
│  Store capabilities (often composed)        │
│  blobs + signed assertions; local pins aside│
├─────────────────────────────────────────────┤
│  Data Model                                 │
│  Trible (64 bytes) → TribleSet (6 indexes)  │
└─────────────────────────────────────────────┘
```

1. **Data model** – the immutable trible structures that encode facts.
2. **Stores** – generic blob and branch-assertion capabilities that abstract over persistence backends; mutable local pins are a separate capability.
3. **Repository** – the coordination layer that combines stores into a versioned history.
4. **Workspaces** – the in‑memory editing surface used by applications and tools.

Each layer has a tight, well defined boundary.  Code that manipulates tribles never needs to know if bytes ultimately land on disk or in memory, and repository level operations never reach inside the data model.  This separation keeps interfaces small, allows incremental optimisation and makes it easy to swap pieces during experimentation.

## Data Model

The fundamental unit of information is a [`Trible`](https://docs.rs/triblespace/latest/triblespace/trible/struct.Trible.html).  Its 64 byte layout is described in [Trible Structure](deep-dive/trible-structure.md).  A `Trible` links a subject entity to an attribute and value.  Multiple tribles are stored in a [`TribleSet`](https://docs.rs/triblespace/latest/triblespace/trible/struct.TribleSet.html), which behaves like a hashmap with three columns — subject, attribute and value.

The 64 byte boundary allows tribles to live comfortably on cache lines and makes deduplication trivial.  Because tribles are immutable, the runtime can copy, hash and serialise them without coordinating with other threads.  Higher level features like schema checking and query evaluation are therefore free to assume that every fact they observe is stable for the lifetime of a query.

## Trible Sets

`TribleSet`s provide fast querying and cheap copy‑on‑write semantics.  They can be merged, diffed and searched entirely in memory.  When durability is needed the set is serialised into a blob and tracked by the repository layer.

To keep joins skew‑resistant, each set maintains all six orderings of entity,
attribute and value.  The trees reuse the same leaf nodes so a trible is stored
only once, avoiding a naïve six‑fold memory cost while still letting the search
loop pick the most selective permutation using the constraint heuristics.

## Blob Storage

All persistent data lives in a [`BlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/index.html).  Each blob is addressed by the hash of its contents, so identical data occupies space only once and readers can verify integrity by recomputing the hash.  The trait exposes simple `get` and `put` operations, leaving caching and eviction strategies to the backend.  Implementations decide where bytes reside: an in‑memory [`MemoryBlobStore`](https://docs.rs/triblespace/latest/triblespace/blob/struct.MemoryBlobStore.html), an on‑disk [`Pile`](https://docs.rs/triblespace/latest/triblespace/repo/pile/struct.Pile.html) described in [Pile Format](pile-format.md) or a remote object store.  Because handles are just 32‑byte hashes, repositories can copy or cache blobs without coordination.  Trible sets, user blobs and commit records all share this mechanism.

Content addressing also means that blob stores can be layered.  Applications commonly use a fast local cache backed by a slower durable store.  Only the outermost layer needs to implement eviction; inner layers simply re-use the same hash keys, so cache misses fall through cleanly.

## Branch Assertions

A branch is identified by the complete pair `(author Ed25519 key, name blob
handle)`. The derived 16-byte `BranchId` is an index prefix, never identity
equality. This matters both for collision safety and because two authors may use
the same human-readable name without sharing a branch.

[`BranchAssertionStore`](https://docs.rs/triblespace/latest/triblespace/repo/branch_assertion/trait.BranchAssertionStore.html)
stores a grow-only set of signed `(identity, commit)` assertions. Appending the
same assertion is idempotent; there is no replacement, tombstone, ordering by
arrival, or scalar authoritative head. Physical pile order is storage history,
not branch precedence.

Resolution compares asserted commits through the commit DAG and retains the
maximal ancestry frontier. It reports four states explicitly:

- **absent** — no assertion exists for the exact identity;
- **tip-pending** — an asserted surviving tip is not locally readable;
- **partial** — tips are readable, but missing ancestry prevents an exact
  dominance decision; and
- **complete** — the maximal antichain is known.

A complete singleton resolves to its existing commit. A divergent complete
frontier resolves for reading to one deterministic, flat, authorless synthetic
merge over all maximal tips. That merge is a derived view, not replicated
branch state. Partial frontiers expose only a candidate-root
descriptor: they cannot be checked out or license a new authored merge
assertion until ancestry is complete.

Mutable [`PinStore`](https://docs.rs/triblespace/latest/triblespace/repo/trait.PinStore.html)
entries still exist for local retention and transport bookkeeping. They are not
branches and must not be interpreted or replicated as branch authority.

## Repository

The [`Repository`](https://docs.rs/triblespace/latest/triblespace/repo/struct.Repository.html) combines a blob store, a local authoring key, and—when the backend supports it—a branch-assertion store. Commits store content and parent metadata as immutable blobs. Because everything is content addressed, multiple repositories can share blobs without coordinating their placement.

Repository logic performs a few critical duties:

- **Validation** – ensure published tips are canonical commits and branch assertions carry valid signatures.
- **Blob synchronization** – upload staged data through the content-addressed blob store, which skips
  already-present bytes and reports integrity errors.
- **Frontier resolution** – use commit ancestry to derive the maximal asserted tips without assigning meaning to assertion arrival order.
- **History traversal** – provide iterators that let clients walk commit DAGs efficiently.

All of these operations rely only on hashes and immutable blobs, so repositories can be mirrored easily and verified offline.

## Workspaces

A [`Workspace`](https://docs.rs/triblespace/latest/triblespace/repo/struct.Workspace.html) represents mutable state during editing. Creating an unpublished workspace or pulling a complete frontier yields a workspace backed by a fresh `MemoryBlobStore`. Commits are created locally and only become branch state when a signed assertion is published.

Workspaces behave like sandboxes.  They host application caches, pending trible sets and user blobs.  Because they speak the same blob language as repositories, synchronisation is just a matter of copying hashes from the workspace store into the shared store once a commit is finalised.

## Commits and History

`TribleSet`s written to blobs form immutable commits. Each commit references
zero or more parents, creating an append-only content-addressed DAG. This is the
durable history shared between repositories.

Because commits are immutable, rollback and branching are cheap. Diverging
histories coexist as a maximal frontier. Reading can use the deterministic
synthetic flat merge; publishing a descendant turns an intentional
reconciliation into another immutable commit and signed assertion.

## Putting It Together

```text
+---------------------------------------------------+
|                    Repository                     |
|  BlobStore (content addressed)                    |
|  BranchAssertionStore (grow-only signed set)      |
+-----------------------------+---------------------+
          ^ push assertion       resolve / pull
          |                              |
          |                              v
+-----------------------------+---------------------+
|                    Workspace                      |
|  base blob snapshot + staged MemoryBlobStore      |
|  exact identity + proposed commit                 |
+-----------------------------+---------------------+
        ^             ^               |
     commit        add_blob         checkout
        |             |               v
+---------------------------------------------------+
|                    Application                    |
+---------------------------------------------------+
```

`Repository::resolve` snapshots assertions and classifies an exact identity's
frontier. `Repository::pull` opens only a complete frontier as a writable
workspace; if it is divergent, the workspace starts from the derived flat
merge. Workspace methods stage blobs and commits locally. `Repository::push`
first copies and flushes every staged blob, validates the proposed commit, then
durably appends one signed assertion. There is deliberately no compare-and-set
race and no hidden conflict winner.

The boundaries between layers encourage modular tooling. A CLI client can
operate entirely within a workspace while transport moves immutable blobs.
An assertion-native synchronizer can separately union signed assertions once
it implements exact identity, verification, admission, and durability; the
current `triblespace-net` migration bridge does not yet do so. As long as
components honour the blob and assertion contracts they can evolve
independently without risking the core guarantees of TribleSpace.
