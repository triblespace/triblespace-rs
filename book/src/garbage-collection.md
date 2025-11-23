# Garbage Collection and Forgetting

Repositories grow over time as commits, branch metadata, and user blobs
accumulate. Because every blob is content addressed and immutable, nothing is
ever overwritten and there is no automatic reclamation when branches move or
objects become orphaned. To keep disk usage in check a repository can
periodically _forget_ blobs that are no longer referenced.

Forgetting is deliberately conservative. It only removes local copies, so
re-synchronising from a peer or pushing a commit that references an "forgotten"
blob will transparently restore it. Forgetting therefore complements the
monotonic model: history never disappears globally, but any node can opt-out of
retaining data it no longer needs.

The main challenge is deciding which blobs are still reachable without
reconstructing every `TribleSet`. The sections below outline how the repository
module solves that problem and how you can compose the building blocks in your
own tools.

## Understanding the Roots

The walk begins with a _root set_—the handles you know must stay alive. In a
typical repository this includes the metadata blob for each branch (which in
turn names the commit heads), tags, or any additional anchors your deployment
requires. Roots are cheap to enumerate: walk the branch store via
[`BranchStore::branches`](https://docs.rs/tribles/latest/tribles/repo/trait.BranchStore.html#tymethod.branches)
and load each branch head, or read the subset of metadata relevant to the
retention policy you are enforcing. Everything reachable from those handles
will be retained by the traversal; everything else is eligible for forgetting.

## Conservative Reachability

Every commit and branch metadata record is stored as a `SimpleArchive`. The
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

1. Enumerate all branches and load their metadata blobs.
2. Extract candidate handles from the metadata. This reveals the current commit
   head along with any other referenced blobs.
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
duplicates whichever handles you feed it. The in-memory `MemoryBlobStore` can
retain live blobs, duplicate them into a scratch store, and report how many
handles were touched without writing bespoke walkers:

```rust
use triblespace::core::blob::memoryblobstore::MemoryBlobStore;
use triblespace::core::repo::{self, BlobStoreKeep, BlobStoreList, BranchStore};
use triblespace::core::value::schemas::hash::Blake3;

let mut store = MemoryBlobStore::<Blake3>::default();
// ... populate the store or import data ...

let mut branch_store = /* your BranchStore implementation */;
let reader = store.reader()?;

// Collect the branch metadata handles we want to keep alive.
let mut roots = Vec::new();
for branch_id in branch_store.branches()? {
    if let Some(meta) = branch_store.head(branch_id?)? {
        roots.push(meta.transmute());
    }
}

// Trim unreachable blobs in-place.
store.keep(repo::reachable(&reader, roots.clone()));

// Optionally copy the same reachable blobs into another store.
let mut scratch = MemoryBlobStore::<Blake3>::default();
let visited = repo::reachable(&reader, roots.clone()).count();
let mapping: Vec<_> = repo::transfer(
    &reader,
    &mut scratch,
    repo::reachable(&reader, roots),
)
.collect::<Result<_, _>>()?;

println!("visited {} blobs, copied {}", visited, mapping.len());
println!("rewrote {} handles", mapping.len());
```

In practice you will seed the walker with the handles extracted from branch
metadata or other root sets instead of iterating the entire store. The helper
takes any `IntoIterator` of handles, so once branch heads (and other roots) have
been identified, they can be fed directly into the traversal without writing
custom queues or visitor logic. Passing the resulting iterator to
`MemoryBlobStore::keep` or `repo::transfer` makes it easy to implement
mark-and-sweep collectors or selective replication pipelines without duplicating
traversal code.

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

## Future Work

The public API for triggering garbage collection is still evolving. The
composition-friendly walker introduced above is one building block; future work
could layer additional convenience helpers or integrate with external retention
policies. Conservative reachability by scanning `SimpleArchive` bytes remains
the foundation for safe space reclamation.
