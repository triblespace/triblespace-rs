# PATCH

The **Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance**
(PATCH) is Trible Space’s workhorse for set operations. It combines three core
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

## Hash maintenance

Every leaf stores a SipHash-2-4 fingerprint of its key, and each branch XORs
its children’s 128-bit hashes together. On insert or delete the previous hash
contribution is XORed out and the new value XORed in, so updates run in constant
time. Set operations such as `difference` compare these fingerprints first:
matching hashes short-circuit because the subtrees are assumed identical, while
differing hashes force a walk to compute the result. SipHash collisions are
astronomically unlikely for these 128-bit values, so the shortcut is safe in
practice.

Consumers can reorder or segment keys through the [`KeySchema`](../../src/patch.rs)
and [`KeySegmentation`](../../src/patch.rs) traits. Prefix queries reuse the
schema’s tree ordering to walk just the matching segments. Because every update
is implemented with copy-on-write semantics, cloning a tree is cheap and retains
structural sharing: multiple workspaces can branch, mutate independently, and
merge results without duplicating entire datasets.
