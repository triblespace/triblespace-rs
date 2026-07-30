# PATCH

The **Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance**
(PATCH) is TribleSpace’s workhorse for set operations. It combines three core
ideas:

1. **Persistence.** Updates clone only the modified path, so existing readers
   keep a consistent view while writers continue mutating. The structure behaves
   like an immutable value with copy-on-write updates.
2. **Adaptive width.** Every node is conceptually 256-ary, yet the physical
   footprint scales with the number of occupied children.
3. **Hash maintenance.** Each subtree carries a 128-bit fingerprint that allows
   set operations to skip identical branches early.

Together these properties let PATCH evaluate unions, intersections, and
differences quickly while staying cache friendly and safe to clone.

## Node layout

Traditional Adaptive Radix Trees (ART) use specialised node types (`Node4`,
`Node16`, `Node48`, …) to balance space usage against branching factor. PATCH
instead stores every branch in the same representation:

* The `Branch` header tracks the first depth where the node diverges
  (`end_depth`) and caches a pointer to a representative child leaf
  (`childleaf`). These fields give PATCH its path compression — a branch can
  cover several key bytes, and we only expand into child tables once the
  children disagree below `end_depth`.
* Children live in a byte-oriented cuckoo hash table backed by a single
  slice of `Option<Head>`. Each bucket holds two slots and the table grows in
  powers of two up to 256 entries.
* A heap `Leaf` owns its key and value. An archive-backed `LocalLeaf` is instead
  a tagged pointer directly into immutable archive bytes. It has no allocation
  or reference count of its own; the enclosing `PATCH` keeps the archive alive.

Insertions reuse the generic `modify_child` helper, which drives the cuckoo loop
and performs copy-on-write if a branch is shared. When the existing allocation
is too small we allocate a larger table with the same layout, migrate the
children, and update the owning pointer in place. Because every branch uses the
same structure we avoid the tag soup and pointer chasing that ARTs rely on while
still adapting to sparse and dense fan-out.

## Resizing strategy

PATCH relies on two hash functions: an identity map and a pseudo-random
permutation sampled once at startup. Both hashes feed a simple compressor that
masks off the unused high bits for the current table size. Doubling the table
therefore only exposes one more significant bit, so each child either stays in
its bucket or moves to the partner bucket `index + old_bucket_count`.

The `byte_table_resize_benchmark` demonstrates how densely the table can fill
before resizing. The benchmark inserts all byte values repeatedly and records the
occupancy that forced each power-of-two table size to grow:

```
ByteTable resize fill - random: 0.863, sequential: 0.972
Per-size fill (random)
  size   2: 1.000  # path compression keeps two-entry nodes fully occupied
  size   4: 0.973
  size   8: 0.899
  size  16: 0.830
  size  32: 0.749
  size  64: 0.735
  size 128: 0.719
  size 256: 1.000  # identity hash maps all 256 children without resizing
Per-size fill (sequential)
  size   2: 1.000  # path compression keeps two-entry nodes fully occupied
  size   4: 1.000
  size   8: 0.993
  size  16: 1.000
  size  32: 0.928
  size  64: 0.925
  size 128: 0.927
  size 256: 1.000  # identity hash maps all 256 children without resizing
```

Random inserts average roughly 86 % table fill while sequential inserts stay
near 97 % before the next doubling. Small nodes stay compact because the
path-compressed header only materialises a table when needed, while the largest
table reaches full occupancy without growing past 256 entries. These predictable fill
factors keep memory usage steady without ART’s specialised node types.

## Archive-backed leaf lifetimes

A `LocalLeaf` is safe only while the allocation containing its bytes remains
alive. Each `PATCH` therefore carries an exact persistent owner cover: a binary
Patricia trie keyed by the data address of each retained `Arc<dyn
ArchiveOwner>`. Retaining the owner also prevents its address from being reused.
The cover is deduplicated by address and structurally shared across snapshots.
Its governing invariant is

```text
owners(LocalLeaves(root)) ⊆ cover
```

Structural operations preserve that invariant as follows:

* archive insertion retains the owner before publishing its `LocalLeaf`;
* cloning clones the root and cover together;
* union joins both covers before either root is moved or detached;
* intersection retains both input covers, because it may reuse a leaf from
  either side;
* difference retains the left cover, because it can only reuse left-hand
  leaves; and
* consuming iterators carry the cover beside their detached traversal queue
  until every queued key has been copied out or dropped.

The cover is a lifetime receipt rather than a reachability index. Operations
may conservatively retain an owner whose leaves disappeared from the result;
direct clearing and removal paths that empty a `PATCH` clear that provenance.
Aggregate reconciliation may nevertheless install or retain a conservative
cover on an already-empty `PATCH`, so emptiness alone does not promise immediate
release. A `TribleSet` shares one such cover across all six PATCH indexes,
joining any divergent covers once at aggregate set-operation boundaries.

## Hash maintenance

On first use in a process, PATCH samples a private random key. Each leaf
fingerprint is the 128-bit output of SipHash-2-4 under that key, and each branch
stores the XOR of its children’s fingerprints. On insert or delete, the old
contribution is XORed out and the new one XORed in, so aggregate maintenance is
constant-time. Set operations compare aggregates first: equal fingerprints
short-circuit under the practical assumption that they denote equal key sets,
while unequal ones force a structural walk. For any fixed pair of unequal sets,
the false-positive probability is approximately 2^-128 under the keyed-hash
assumption.

The raw subtree fingerprints are process-local implementation values, not
serialized identities. They must remain opaque to untrusted chosen-input
callers. Although XOR is linear, the usual linear-dependency construction
requires observing the fingerprints of chosen keys; the private key makes that
attack inapplicable without such an exposure oracle. PATCH's raw root aggregate
therefore stays crate-private.

`TribleSet::fingerprint` preserves the useful O(1) public cache-key API without
opening that oracle. It applies a domain-separated SipHash-2-4 PRF to the root
aggregate under a second process-random key initialized beside the leaf key.
`TribleSetFingerprint::as_u128`, `Debug`, and `Hash` expose only this nonlinear
blinding. Equal sets retain equal tokens within one process, while the XOR of
public singleton tokens reveals nothing useful about the aggregate of their
union. The token remains a 128-bit cache hint, not a durable content identifier
or proof of equality.

Archive-backed leaves do not cache their fingerprint, so PATCH avoids hashing
them when an exact, cheaper decision is available. Pairs of leaf nodes involving
a `LocalLeaf` compare key bytes directly. A `LocalLeaf` paired with a subtree of
cardinality other than one rejects fingerprint equality from the cached count;
a unary branch remains eligible for the ordinary fingerprint path. These are
performance shortcuts only, not collision remediation. Pairs without a
`LocalLeaf` retain the normal cached-fingerprint path.

Consumers can reorder or segment keys through the [`KeySchema`](../../src/patch.rs)
and [`KeySegmentation`](../../src/patch.rs) traits. Prefix queries reuse the
schema’s tree ordering to walk just the matching segments. Because every update
is implemented with copy-on-write semantics, cloning a tree is cheap and retains
structural sharing: multiple workspaces can branch, mutate independently, and
merge results without duplicating entire datasets.
