# Garbage Collection and Forgetting

Repositories grow over time as commits, generic pin assertions, and user blobs
accumulate. Because every blob is content addressed and immutable, nothing is
overwritten in place. A retention backend such as `Yard` can periodically
_forget_ blobs that are not reachable from durable roots, while a raw `Pile`
remains append-only until an explicit rewrite.

Forgetting is deliberately conservative. It only removes local copies, so an
explicit low-level forget can leave a branch `TipPending` until a peer restores
its asserted target. Automatic repository collection is stricter for recognized
branch pins: every valid assertion whose locally present descriptor decodes as
a canonical `BranchPinDescriptor` roots that descriptor, its name, its commit
target, and the target's locally present closure.

The main challenge is deciding which blobs are still reachable without
reconstructing every `TribleSet`. The sections below outline how the repository
module solves that problem and how you can compose the building blocks in your
own tools.

## Understanding the Roots

The walk begins with a _root set_—the handles you know must stay alive. Generic
asserted-pin storage itself is kind-neutral: an unknown descriptor never turns
its opaque value into a permanent content root. The branch adapter recognizes a
branch only when the pin's descriptor blob is locally present and decodes as
the canonical `BranchPinDescriptor`. It then treats every valid assertion's
commit target as a hard root. Collectors must use all such assertions, not only
a branch's currently resolved frontier: missing ancestry can make domination
impossible to prove, and collection must not turn uncertainty into deletion.
Admission policy and quota bound which remote assertions become accepted;
pressure never silently weakens accepted state.

The content-addressed descriptor and name handle are retained directly. The
name is not traversed as a generic blob graph: arbitrary
`LongString` bytes do not acquire reference semantics merely because they
contain a 32-byte sequence. Commit targets, in contrast, are traversed
recursively, retaining parents, content, metadata, messages, attachments, and
any other locally present referenced blobs.

`Yard` also derives soft cache roots from signed asserted wants. It recognizes
the fixed `WantPinDescriptor`, unions authentic values across all authors,
orders those exact handles canonically, discards values that are not locally
present, and retains up to `YardConfig::want_budget`. These soft roots retain
only the named blobs; they never veto or weaken a branch hard root or its
closure. Satisfaction, budget eviction, and local absence do not erase the
underlying assertions—the want view is a grow-only set.

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

1. Take one coherent snapshot of all generic pin assertions.
2. For each valid assertion, load its descriptor locally. If it is a canonical
   `BranchPinDescriptor`, retain the descriptor and name directly and add the
   assertion's commit value to the hard root set. Preserve unknown assertion
   records without treating their values as branch roots.
3. Independently project authentic `WantPinDescriptor` values across every
   author. Retain up to the configured budget of canonically ordered, locally
   present exact values as soft cache roots; do not traverse them as branch
   structure.
4. Recursively walk the discovered commits and content blobs. Each blob is
   scanned in 32-byte steps; any chunk whose lookup succeeds is enqueued instead
   of deserialising the archive.
5. Stream the discovered handles into whatever operation you need. The
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
generic assertions can derive branch roots by projecting the typed descriptor
from its assertion snapshot:

```rust,ignore
use triblespace::core::blob::encodings::{longstring::LongString, UnknownBlob};
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::inline::encodings::hash::Handle;
use triblespace::core::inline::Inline;
use triblespace::core::repo::branch_pin::{commit_from_value, BranchPinDescriptor};
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::pin_assertion::PinAssertionStore;
use triblespace::core::repo::{self, BlobStore, BlobStoreGet, BlobStoreKeep};

let mut store = MemoryRepo::default();
// ... populate the store or import data ...

let assertions = store.pin_assertion_snapshot()?;
let reader = store.reader()?;
let mut commit_roots = Vec::<Inline<Handle<UnknownBlob>>>::new();
let mut direct = Vec::<Inline<Handle<UnknownBlob>>>::new();

for assertion in assertions.iter() {
    let descriptor = Inline::<Handle<BranchPinDescriptor>>::new(
        assertion.identity().pin().raw(),
    );
    let Ok(name) = reader.get::<Inline<Handle<LongString>>, BranchPinDescriptor>(descriptor)
    else {
        // Missing or unknown descriptors stay in the assertion ledger but do
        // not lend branch-root semantics to an opaque value.
        continue;
    };

    direct.push(descriptor.transmute());
    direct.push(name.transmute());
    commit_roots.push(commit_from_value(assertion.value()).transmute());
}

// Walk commit structure, then retain descriptors and names directly without
// treating arbitrary LongString bytes as edges in the blob graph.
let live: Vec<_> = repo::reachable(&reader, commit_roots)
    .chain(direct)
    .collect();
store.keep(live.iter().copied());

// Optionally copy the same reachable blobs into another store. `transfer`
// reports an error if a selected target, descriptor, or name is not local.
let mut scratch = MemoryBlobStore::default();
let visited = live.len();
let mapping: Vec<_> = repo::transfer(&reader, &mut scratch, live)
    .collect::<Result<_, _>>()?;

println!("visited {} blobs, copied {}", visited, mapping.len());
println!("rewrote {} handles", mapping.len());
```

In practice you seed the walker with every recognized branch-pin target plus
any additional application roots. The helper takes any `IntoIterator` of handles, so those
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
- **Rewrite asserted-pin records atomically.** A physical Pile replacement must
  write the segment's complete generic assertion set—including structurally
  admitted witnesses of unknown kinds—into the temporary Pile before the atomic
  rename. Re-appending assertions afterwards creates a crash window in which
  accepted grow-only state has vanished.
- **Treat wants as durable intent, not mutable cache state.** Reading a raw
  `Pile` or `Yard` never creates a want. An authoring wrapper records one on a
  miss, while successful retrieval and later eviction leave the assertion
  intact.
- **Pair finite budgets with an explicit service policy.** Grow-only wants
  deliberately persist. If one author has more serviceable wants than
  `want_budget`, collection can evict an unbudgeted blob and reconciliation can
  fetch it again. The current implementation does not yet define a typed
  physical-forgetting operation for selected wants or a service-selection
  policy. Until one exists, the budget alone cannot prevent this
  collect/reconcile oscillation.

## Future Work

The public API for triggering garbage collection is still evolving. The
composition-friendly walker introduced above is one building block; future work
could layer additional convenience helpers or integrate with external retention
policies. Conservative reachability by scanning `SimpleArchive` bytes remains
the foundation for safe space reclamation.
