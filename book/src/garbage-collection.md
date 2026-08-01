# Garbage Collection and Forgetting

Repositories grow over time as commits, branch assertions, and user blobs
accumulate. Because every blob is content addressed and immutable, nothing is
overwritten in place. A retention backend such as `Yard` can periodically
_forget_ blobs that are not reachable from durable roots, while a raw `Pile`
remains append-only until an explicit rewrite.

Forgetting is deliberately conservative. It only removes local copies, so an
explicit low-level forget can leave an assertion `TipPending` until a peer
restores its target. Automatic repository collection is stricter: every
accepted branch assertion is a durable root, so its locally present name,
commit target, and target closure survive collection.

The main challenge is deciding which blobs are still reachable without
reconstructing every `TribleSet`. The sections below outline how the repository
module solves that problem and how you can compose the building blocks in your
own tools.

## Understanding the Roots

The walk begins with a _root set_—the handles you know must stay alive. For an
assertion-native repository, every assertion's commit target is a hard root.
Collectors must use all accepted assertions, not only a branch's currently
resolved frontier: missing ancestry can make domination impossible to prove,
and collection must not turn uncertainty into deletion. Admission policy and
quota bound which remote assertions become accepted; pressure never silently
weakens accepted state.

The content-addressed name handle in each exact branch descriptor is retained
directly when present. It is not traversed as a generic blob graph: arbitrary
`LongString` bytes do not acquire reference semantics merely because they
contain a 32-byte sequence. Commit targets, in contrast, are traversed
recursively, retaining parents, content, metadata, messages, attachments, and
any other locally present referenced blobs.

`Yard` also has demand-born weak pins for cache wants. They may veto legacy
strong-pin reachability, but they do not veto an assertion root or anything in
its closure. A common arrival order is assertion, failed read (which creates a
weak want), then fetched commit; the later collection must retain that commit,
not interpret its old want marker as permission to erase published history.

## Conservative Reachability

Every commit and structured application-metadata record is stored as a
`SimpleArchive`. The
archive encodes a canonical `TribleSet` as 64-byte tribles, each containing a
32-byte value column. The blob store does not track which handles correspond to
archives, so the collector treats every blob identically: it scans the raw bytes
in 32-byte chunks and treats each chunk as a candidate handle. Chunks that are
not value columns—for example the combined entity/attribute half of a trible or
arbitrary attachment bytes—are discarded when the candidate lookup fails. If a
chunk matches the hash of a blob in the store we assume it is a reference,
regardless of the attribute type. With 32-byte hashes the odds of a random
collision are negligible, so the scan may keep extra blobs but will not drop a
referenced one.

Content blobs that are not `SimpleArchive` instances (for example large binary
attachments) therefore behave as leaves: the traversal still scans them, but
because no additional lookups succeed they contribute no further handles. They
become reachable when some archive references their handle and are otherwise
eligible for forgetting.

## Traversal Algorithm

1. Take one coherent snapshot of all accepted branch assertions.
2. Add every asserted commit target to the hard root set and retain each
   descriptor's name handle directly when it is present.
3. Recursively walk the discovered commits and content blobs. Each blob is
   scanned in 32-byte steps; any chunk whose lookup succeeds is enqueued instead
   of deserialising the archive.
4. Stream the discovered handles into whatever operation you need. The
   [`reachable`](https://docs.rs/triblespace/latest/triblespace/repo/fn.reachable.html)
   helper returns an iterator of handles, so you can retain them, transfer
   them into another store, or collect them into whichever structure your
   workflow expects.

Because the traversal is purely additive you can compose additional filters or
instrumentation as needed—for example to track how many objects are held alive
by a particular branch or to export a log of missing blobs for diagnostics.

## Automating the Walk

The repository module already provides most of the required plumbing. The
[`reachable`](https://docs.rs/triblespace/latest/triblespace/repo/fn.reachable.html)
helper exposes the traversal as a reusable iterator so you can compose other
operations along the way, while
[`transfer`](https://docs.rs/triblespace/latest/triblespace/repo/fn.transfer.html)
duplicates whichever handles you feed it. A store that combines blobs and
assertions can derive the hard roots directly from its assertion snapshot:

```rust,ignore
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::repo::branch_assertion::BranchAssertionStore;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::{self, BlobStore, BlobStoreKeep};

let mut store = MemoryRepo::default();
// ... populate the store or import data ...

let assertions = store.assertion_snapshot()?;
let reader = store.reader()?;
let commit_roots: Vec<_> = assertions
    .iter()
    .map(|assertion| assertion.commit().transmute())
    .collect();
let names: Vec<_> = assertions
    .iter()
    .map(|assertion| assertion.identity().name().transmute())
    .collect();

// Walk only commit structure, then retain names directly without treating
// arbitrary LongString bytes as edges in the blob graph.
store.keep(
    repo::reachable(&reader, commit_roots.clone()).chain(names.iter().copied()),
);

// Optionally copy the same reachable blobs into another store. `transfer`
// reports an error if an asserted target or name is not materialised locally.
let mut scratch = MemoryBlobStore::default();
let visited = repo::reachable(&reader, commit_roots.clone()).count() + names.len();
let mapping: Vec<_> = repo::transfer(
    &reader,
    &mut scratch,
    repo::reachable(&reader, commit_roots).chain(names),
)
.collect::<Result<_, _>>()?;

println!("visited {} blobs, copied {}", visited, mapping.len());
println!("rewrote {} handles", mapping.len());
```

In practice you seed the walker with every asserted target plus any additional
application roots. The helper takes any `IntoIterator` of handles, so those
roots can be fed directly into the traversal without writing custom queues or
visitor logic. Passing the resulting iterator to `MemoryBlobStore::keep` or
`repo::transfer` makes it easy to implement
mark-and-sweep collectors or selective replication pipelines without duplicating
traversal code. The compact transfer example deliberately treats an incomplete
assertion closure as an error; a synchroniser can instead report missing direct
names and roots as wants while transferring the locally present subset.

When you already have metadata represented as a `TribleSet`, the
[`potential_handles`](https://docs.rs/triblespace/latest/triblespace/repo/fn.potential_handles.html)
helper converts its value column into the conservative stream of
`Handle<H, UnknownBlob>` instances expected by these operations.

## Operational Tips

- **Schedule forgetting deliberately.** Trigger it after large merges or
  imports rather than on every commit so you amortise the walk over meaningful
  changes.
- **Watch available storage.** Because forgetting only affects the local node,
  replicating from a peer may temporarily reintroduce forgotten blobs. Consider
  monitoring disk usage and budgeting headroom for such bursts.
- **Keep a safety margin.** If you are unsure whether a handle should be
  retained, include it in the root set. Collisions between 32-byte handles are
  effectively impossible, so cautious root selection simply preserves anything
  that might be referenced.
- **Rewrite assertion records atomically.** A physical Pile replacement must
  write the segment's complete assertion set into the temporary Pile before the
  atomic rename. Re-appending assertions afterwards creates a crash window in
  which accepted grow-only state has vanished.

## Future Work

The public API for triggering garbage collection is still evolving. The
composition-friendly walker introduced above is one building block; future work
could layer additional convenience helpers or integrate with external retention
policies. Conservative reachability by scanning `SimpleArchive` bytes remains
the foundation for safe space reclamation.
