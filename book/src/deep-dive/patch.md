# PATCH

The **Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance**
(PATCH) is TribleSpace’s workhorse for set operations. It combines three core
ideas:

1. **Persistence.** Updates clone only the modified path, so existing readers
   keep a consistent view while writers continue mutating. The structure behaves
   like an immutable value with copy-on-write updates.
2. **Adaptive width.** Every node is conceptually 256-ary, yet the physical
   footprint scales with the number of occupied children.
3. **Hash receipts.** Subtrees can carry an exact 128-bit fingerprint that lets
   set operations skip identical branches early. Unknown receipts stay
   conservative instead of forcing archive-backed leaves to be hashed during
   every structural update.

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

## Archive-backed leaves

An ordinary `Leaf` owns its key and value. A `LocalLeaf` instead points directly
at immutable, aligned key bytes in an archive. The enclosing PATCH retains an
exact persistent set of archive owners at its root; every LocalLeaf reachable
from that root is covered by an owner, and retaining extra owners is harmless.
This keeps archive lifetime independent of trie shape, so copy-on-write,
resizing, and set operations can move Heads without reifying their bytes into
heap leaves.

The owner set is a Patricia trie keyed by allocation address. Its shape is
canonical for an address set, its height is bounded by the machine word width,
and unchanged paths remain shared across PATCH snapshots. A PATCH drops its
root before its owner set, ensuring no archive pointer can outlive the guard
that makes it readable.

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

## Hash receipts

Heap leaves store a SipHash-2-4 fingerprint of their key. Archive-backed
LocalLeaves deliberately do not: their canonical representation is just the
archive bytes. A branch's receipt obeys the one-way invariant

```
branch.hash == 0  OR  branch.hash == XOR(hash(key) for key below branch)
```

Zero is conservative cache bottom. It may also be the genuine XOR of a
nonempty subtree; that rare case is simply treated as unknown and recomputed on
demand. A nonzero cached parent may cover dirty children when its exact receipt
was derived algebraically, so cache knowledge is not required to be
downward-closed.

A child mutation returns its structural result together with an exact
fingerprint delta when the operation knows one:

```
delta = H(keys before) xor H(keys after)
H(parent after) = H(parent before) xor delta
```

Thus a resident parent can remain exact across a duplicate insertion, failed
removal, value-only replacement, or insertion/removal whose leaf fingerprint
is supplied or resident, even when the changed child itself is dirty.
`delta == 0` describes equal key fingerprints, not structural identity or value
provenance; the returned child must still be installed. When the operation
cannot supply a delta, the child editor falls back to already-resident old and
replacement receipts. If neither source is available, the parent becomes
dirty. Structural mutation never calls `child.hash()` merely to maintain a
cache. Reading a resident root hash is O(1); reading a dirty root folds the
subtree in O(n) and does not memoize through a shared reference.

Serial union and the small parallel fallback repair more receipts without
hashing disjoint LocalLeaves. For

```
H(S) = XOR(hash(key) for key in S),
```

the recursive union also returns the intersection receipt and applies

```
H(A union B) = H(A) xor H(B) xor H(A intersection B).
```

Disjoint byte partitions contribute zero; equal singleton keys contribute one
leaf hash; child intersections combine by XOR. When both input roots are
resident, this is enough to make the result root resident even if newly formed
internal branches remain dirty.

The large parallel scatter path still rebuilds aggregates eagerly and may hash
dirty children. Carrying the same overlap receipts through its disjoint Rayon
buckets is a separate implementation step; it does not change the serial
algebra or cache invariant above.

Set operations such as `difference` use whole-subtree equality shortcuts only
when both candidate receipts are resident. Dirty trees descend structurally
instead of forcing hashes merely to ask whether they match. SipHash collisions
remain the same probabilistic equality assumption as before and are
astronomically unlikely for these 128-bit values.

Consumers can reorder or segment keys through the [`KeySchema`](../../src/patch.rs)
and [`KeySegmentation`](../../src/patch.rs) traits. Prefix queries reuse the
schema’s tree ordering to walk just the matching segments. Because every update
is implemented with copy-on-write semantics, cloning a tree is cheap and retains
structural sharing: multiple workspaces can branch, mutate independently, and
merge results without duplicating entire datasets.
