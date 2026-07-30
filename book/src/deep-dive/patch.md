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
archive bytes. For the finite canonical key set `S` below a node, define

```
F(S) = XOR(hash(key) for key in S).
```

A branch receipt is either `Unknown` or exactly `Known(F(S))`. The publication
bit is separate from the 128-bit value, so `Known(0)` is not confused with
`Unknown`. An immutable reader that encounters a dirty branch folds its
children once and atomically publishes the result for every shared snapshot.
Composition never hashes a LocalLeaf merely to keep a result warm.

PATCH maintains receipts at two complementary proof boundaries.

First, a Branch partitions its set into disjoint children. If the parent and
every changed contribution are already resident, child mutation updates the
parent by XOR:

```
absent -> N:      parent xor F(N)
O      -> absent: parent xor F(O)
O      -> N:      parent xor F(O) xor F(N)
```

If an earlier prerequisite is unknown, PATCH does not even load the later
child receipt: it cannot complete the proof. The parent becomes dirty instead.
The old child receipt is captured before mutation because a uniquely owned
child Branch may change in place.

Second, a public set operation knows exact cardinalities and inclusion facts
that a generic child editor cannot see. It can publish an exact root above
dirty descendants in these cases:

```
insert/remove one key: unchanged size donates the old root;
insert/remove one key: size delta one XORs the canonical key hash;
union:                result size equals an operand -> donate it;
union:                result size equals the checked operand-size sum -> XOR;
intersection:         result size equals an operand -> donate it;
difference A-B:       unchanged size -> donate A;
difference A-B:       result size + |B| = |A| -> F(A) xor F(B).
```

These rules are complete given only operand/result cardinalities and optional
operand receipts. Write `X=A-B`, `Y=B-A`, and `Z=A intersect B`; cardinality
can eliminate an unknown fingerprint atom only by proving `X`, `Y`, or `Z`
empty, and those are exactly the cases above.

An exact root does not imply exact descendants. A root consumer returns
immediately and leaves hidden dirty children untouched; their local receipts
remain useful information for later recursive set operations. This is why the
Branch-local and operation-boundary laws coexist.

Archive construction computes each row hash once in `ArchiveEntry` and shares
that ephemeral value across all six TribleSet indexes. After the initial
two-row branch, archive and heap insertion use the same generic structural
path; the PATCH boundary combines the row hash with its old root. LocalLeaf
therefore remains one tagged pointer, with no persistent hash descriptor.

Set operations use probabilistic whole-subtree equality only when both
receipts are already resident. Exact persistent-body pointer identity is the
collision-independent shortcut. Dirty trees otherwise descend structurally.
The fingerprint always hashes canonical/shared key bytes; an operation such as
`remove` that accepts a tree-ordered query converts it back through its
`KeySchema` before applying a key-hash delta.

Consumers can reorder or segment keys through the [`KeySchema`](../../src/patch.rs)
and [`KeySegmentation`](../../src/patch.rs) traits. Prefix queries reuse the
schema’s tree ordering to walk just the matching segments. Because every update
is implemented with copy-on-write semantics, cloning a tree is cheap and retains
structural sharing: multiple workspaces can branch, mutate independently, and
merge results without duplicating entire datasets.
