# Garbage Collection and Forgetting

Repositories grow over time as commits, generic pin assertions, and user blobs
accumulate. Because every blob is content addressed and immutable, nothing is
overwritten in place. A retention backend such as `Yard` can periodically
_forget_ blobs that are not reachable from durable roots, while a raw `Pile`
remains append-only until an explicit rewrite.

Forgetting is deliberately conservative. It only removes local copies, so an
explicit low-level forget can leave a branch `TipPending` until a peer restores
its asserted target. Automatic repository collection recognizes the generic
`StrongPinDescriptor`: every valid assertion whose locally present outer
descriptor decodes canonically retains that outer blob and begins hard
propagation from its exact inner descriptor and every distinct asserted value.
An asserted weak pin cuts that propagation. The collector does not need to know
the inner kind.

The main challenge is deciding which blobs are still reachable without
reconstructing every `TribleSet`. The sections below outline how the repository
module solves that problem and how you can compose the building blocks in your
own tools.

## Understanding the Roots

The walk begins with a _root set_—the handles you know must stay alive. Generic
asserted-pin storage itself is kind-neutral: an unknown descriptor never turns
its opaque value into a permanent content root. An exact, locally present
`StrongPinDescriptor` is the explicit opt-in. It grants retention semantics to
its wrapped descriptor and every authentic value, without granting resolution,
authorization, or any other meaning. Collectors use all distinct values, not a
kind-specific resolved frontier: retention must not turn uncertainty into
deletion.
Admission policy and quota bound which remote assertions become accepted;
pressure never silently removes accepted assertions.

Unless it is itself weak-pinned, the outer descriptor is retained directly
because its inner handle begins at byte 16 and is not visible to the aligned
conservative scanner. After exact decoding, Yard explicitly seeds traversal
with the inner handle and asserted values. A weak outer cuts the entire wrapper
effect. A branch's V2 inner descriptor places its name handle at byte 32, so a
locally present name is then retained by ordinary closure discovery. Name bytes
may remain absent without invalidating an exact branch identity.

`Yard` also derives weak boundaries and soft cache roots from signed asserted
wants. It recognizes the fixed `WantPinDescriptor` and unions authentic values
across all authors. Every exact value is a cut point: a hard walk stops before
it, so arrival below a strong root does not silently make the fetched blob hard.
Yard orders those exact handles canonically and selects a global prefix of at
most `YardConfig::want_budget` **before** checking local presence. It retains
only selected values that are present, without traversing their closure. An
absent low-ranked value therefore reserves its slot instead of letting a
present tail value enter a cache frontier that the reconciler cannot reproduce.
Satisfaction, budget eviction, and local absence do not erase the underlying
assertions—the weak-pin view is a grow-only set.

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

1. Take one coherent snapshot of all generic pin assertions and project the
   union of every authentic `WantPinDescriptor` value as the weak cut set.
2. Group valid assertions by outer pin handle. If the outer is weak, skip the
   whole wrapper effect. Otherwise, if the locally present blob decodes as a
   canonical `StrongPinDescriptor`, retain the outer directly and add its exact
   inner handle plus every distinct assertion value to the hard root set.
   Missing or malformed outers are neutral; preserve their assertion records
   unchanged.
3. Walk the discovered hard roots. Before retaining or scanning a handle, stop
   if it belongs to the weak cut set. Otherwise scan the blob in 32-byte steps;
   any chunk whose lookup succeeds is enqueued instead of deserialising the
   archive.
4. Select up to the configured budget of canonically ordered weak handles and
   retain each exact value that is locally present as a soft cache root. Do not
   traverse its closure.
5. Stream the discovered handles into whatever operation you need. The
   [`reachable`](https://docs.rs/triblespace/latest/triblespace/repo/fn.reachable.html)
   helper returns an iterator of handles, so you can retain them, transfer
   them into another store, or collect them into whichever structure your
   workflow expects.

Because the traversal is purely additive you can compose additional filters or
instrumentation as needed—for example to track how many objects are held alive
by a particular branch or to export a log of missing blobs for diagnostics.

## Automating the Walk

The repository module provides most of the required plumbing. The generic
[`reachable`](https://docs.rs/triblespace/latest/triblespace/repo/fn.reachable.html)
helper exposes an unconditional traversal as a reusable iterator. A collector
with weak pins adds the stop predicate described above, while
[`transfer`](https://docs.rs/triblespace/latest/triblespace/repo/fn.transfer.html)
duplicates whichever handles you feed it. A store that combines blobs and
generic assertions can derive hard roots by projecting the generic strong
descriptor from its assertion snapshot:

```rust,ignore
use triblespace::core::blob::encodings::UnknownBlob;
use triblespace::core::blob::MemoryBlobStore;
use triblespace::core::inline::encodings::hash::Handle;
use triblespace::core::inline::Inline;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::pin_assertion::PinAssertionStore;
use triblespace::core::repo::strong_pin::StrongPinDescriptor;
use triblespace::core::repo::{self, BlobStore, BlobStoreGet, BlobStoreKeep};

let mut store = MemoryRepo::default();
// ... populate the store or import data ...

let assertions = store.pin_assertion_snapshot()?;
let reader = store.reader()?;
let mut strong_roots = Vec::<Inline<Handle<UnknownBlob>>>::new();
let mut direct = Vec::<Inline<Handle<UnknownBlob>>>::new();

for assertion in assertions.iter() {
    let outer = StrongPinDescriptor::descriptor_handle(assertion.identity().pin());
    let Ok(inner) = reader.get::<Inline<Handle<UnknownBlob>>, StrongPinDescriptor>(outer)
    else {
        // Missing, malformed, and unwrapped descriptors remain durable but
        // retention-neutral.
        continue;
    };

    direct.push(outer.transmute());
    strong_roots.push(inner);
    strong_roots.push(Inline::new(assertion.value().raw()));
}

// Walk every inner/value closure, retaining the unaligned outer directly.
let live: Vec<_> = repo::reachable(&reader, strong_roots)
    .chain(direct)
    .collect();
store.keep(live.iter().copied());

// Optionally copy the same reachable blobs into another store. `transfer`
// reports an error if selected content is not local.
let mut scratch = MemoryBlobStore::default();
let visited = live.len();
let mapping: Vec<_> = repo::transfer(&reader, &mut scratch, live)
    .collect::<Result<_, _>>()?;

println!("visited {} blobs, copied {}", visited, mapping.len());
println!("rewrote {} handles", mapping.len());
```

In practice you seed the walker with every recognized strong inner descriptor
and asserted value plus any additional application roots. The helper takes any `IntoIterator` of handles, so those
roots can be fed directly into the traversal without writing custom queues or
visitor logic. Passing the resulting iterator to `MemoryBlobStore::keep` or
`repo::transfer` makes it easy to implement
mark-and-sweep collectors or selective replication pipelines without duplicating
traversal code. The compact transfer example deliberately treats an incomplete
assertion closure as an error; a synchroniser can instead report missing roots
as wants while transferring the locally present subset.

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
- **Keep fetch and retention on the same canonical frontier.**
  `WantCachePolicySource` exposes only the artifact's capacity; ordering is not
  configurable. Both collection and reconciliation select the same global
  all-author prefix before presence filtering. `Yard` retains the present
  members, while each peer fetches only its own authored share. Once assertions
  quiesce, background reconciliation cannot fetch a value collection will
  evict. The deliberate trade-off is honest starvation: an unsatisfiable
  low-ranked want can reserve capacity ahead of later values until a future
  explicit retirement mechanism exists.

## Future Work

The public API for triggering garbage collection is still evolving. The
composition-friendly walker introduced above is one building block; future work
could layer additional convenience helpers or integrate with external retention
policies. Conservative reachability by scanning `SimpleArchive` bytes remains
the foundation for safe space reclamation.
