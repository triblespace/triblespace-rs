# Range-Native Rollups

Derived indexes are replaceable views over repository history. They do not
mutate a branch head and they do not have a second mutable "index head". Source
branches and rollups are independent monotone ledgers joined at read time.

The common model has four pieces:

```text
source branch assertions                    rollup assertions
          |                                         |
          v                                         v
authoritative commit frontier       grow-only (hard core, soft node) pairs
          |                                         |
          +------------ read-time join -------------+
                               |
                    resident cover + residual
```

An empty resident cover is a full source read. A complete resident cover is an
indexed read. Missing, evicted, malformed, overlapping, or off-branch nodes can
only enlarge the residual; they cannot omit a source commit. For exact kinds
such as Succinct and Path, repartitioning therefore cannot change the answer.
Ranked or approximate kinds may intentionally expose physical-plan quality;
that distinction belongs to the kind, not to range coverage.

## Inclusive DAG ranges

A commit history is a DAG, so a source region cannot generally be represented
by one linear start and end cursor. Each range has two repeated attributes:

- `commit_start*`: the minimal included commits;
- `commit_end*`: the maximal included commits.

Both frontiers are nonempty antichains and both are inclusive. A one-commit
leaf, including a genesis commit, is `[C,C]`. There is no null or exclusive
cursor sentinel.

For start frontier `S` and end frontier `E`, the range denotes the union of
closed commit-poset intervals:

```text
R(S,E) = { x | some s in S and e in E satisfy s <= x <= e }
```

The frontiers must be exact: `S` is the set of minima of `R(S,E)` and `E` is
its set of maxima. This rejects comparable members within a frontier and
disconnected boundaries.

A diamond illustrates why the values are repeated:

```text
       M
      / \
     A   B
      \ /
       G
```

- the sibling region `{A,B}` has `start={A,B}, end={A,B}`;
- the full diamond has `start={G}, end={M}`;
- the merge commit alone is `start={M}, end={M}` and includes neither parent.

`range_for_commit_set` derives the inclusive boundaries of a nonempty convex
commit batch. For an incremental push, pass exactly the newly reachable
parents-first commits. A previous head is an exclusion cursor, not an inclusive
range start.

## Stable cores and atomic nodes

A `RangeRecord` has an intrinsic entity id derived from
`(index_recipe, commit_start*, commit_end*)`. The recipe identifies the question
and its source parameters: for example a path automaton fingerprint, a BM25
content attribute, or an HNSW source attribute, dimension, and seed.

One published rollup alternative is a pair of content-addressed archives:

```text
core = SimpleArchive(core-only RangeRecord)
node = SimpleArchive(the same RangeRecord + one complete typed artifact bundle)
```

The generic signed assertion stores `core` as its value and `node` as its
opaque label. The rollup descriptor is wrapped by a strong-pin descriptor, so
generic retention follows the small core but does not follow the node label.
Derived bytes therefore remain optional and evictable without a permanent
weak-pin assertion. A completed-empty projection is the degenerate case
`node == core`.

The archive boundary is semantic:

- handles inside one node are conjunctive components or shards;
- different node archives for the same core are disjunctive complete
  alternatives.

Never fact-union alternative nodes merely because their intrinsic range entity
ids match. Doing so could mix a raw Succinct archive from one alternative with
a Rank9 sidecar from another, combine two complete path summaries, or turn
alternative BM25/HNSW segments into one accidental shard bundle. Content
addressing still deduplicates replicas that produce exactly the same node.

## The rollup kind algebra

`repo::index_home::IndexKind` keeps only one semantic associated type,
`Segment`, and five operations:

```text
recipe_fragment()                 identify the question and parameters
build(source facts) -> segments   derive leaves
freeze(segment) -> Fragment       canonical typed facts plus owned blobs
thaw(node) -> segments            atomically validate and attach a bundle
merge(segments) -> segments       compact within one recipe
```

`build` and `merge` return an empty vector for the canonical empty projection.
Each nonempty segment freezes to a nonempty `Fragment` rooted at the range
entity. Generic storage folds those fragments with `+=`, so facts, exports, and
blob stores compose without a prepared/stored/attached state ladder. `thaw` is
all-or-error: a partially resident bundle never enters a cover.

`freeze` is intentionally per segment while `thaw` sees the complete node.
Fragment composition erases segment boundaries, and only the kind knows how
several typed facts form one segment—for example, which raw Succinct archive
pairs with which Rank9 sidecar. A completed-empty node bypasses `thaw`
entirely: its core is structurally known to contain no segment facts.

The trait deliberately does not assert one universal merge homomorphism.
Coverage is exact for every kind; observable query equivalence under a changed
cover is a stronger, kind-specific law. A kind must document whether its
segments are exact representations or quality-bearing physical plans.

The common layer does not claim one universal byte-level merge law:

- **Succinct** stores raw archives with source-bound Rank9 sidecars and exact-
  unions attached archives during compaction.
- **Path** unions constructional summaries. Accepted-path closure happens once
  after resident and residual summaries have all been combined, so paths may
  cross range boundaries repeatedly.
- **BM25** owns segment statistics, scoring, and its merge semantics. The
  current implementation uses per-segment IDF and average document length, so
  cover changes can change scores, ranking, and score-threshold membership even
  though document coverage remains complete.
- **HNSW** owns graph rebuilding, candidate union, and exact rescoring. Rescore
  is exact only over each graph's approximate candidate set, so graph
  repartition can change recall while source-embedding coverage remains exact.

Recipe identity and representation are orthogonal. The recipe says what
question is answered; typed attributes such as `seg_succinct`, `seg_bm25`, or
`seg_hnsw` say how this node represents its answer. Cover homogeneity is by
recipe, not merely by artifact attribute.

## Read-time cover and residual

Resolve the source branch to its authoritative frontier `H` and let `T(H)` be
the union of every tip's ancestor closure. Load independently signed rollup
pairs for that branch and recipe. Only nodes whose core is valid, whose complete
bundle is locally present, and whose segments fully thaw are resident
candidates.

`select_range_cover` validates each candidate's exact members against the
commit DAG, ignores ranges outside `T(H)`, then greedily selects pairwise-
disjoint candidates by descending coverage size and node handle. The policy is
deterministic but not a source-coverage invariant. Whatever it does not select
is returned as an exact list of residual commits:

```text
covered commits union residual commits == T(H)
covered commits intersect residual commits == empty
```

A missing large alternative is omitted before selection, so it cannot starve a
smaller resident node. DAG failures and invalid caller frontiers still fail:
without a trustworthy target set, an exact residual cannot be computed.

Those equations certify complete input coverage, not universal plan
equivalence. Succinct union and Path summary union satisfy the stronger exact
law. Current BM25 and HNSW treat the selected segmentation as part of ranking or
approximation quality; applications that require stable ranked outputs must not
mistake residual exactness for score invariance.

An optional warmer may select a preferred cover from hard cores before nodes
are resident. That is cache policy, not durable query state. Permanent Wants,
negative assertions, and replacement records are unnecessary.

## Monotone compaction

Compaction chooses pairwise-disjoint victim nodes whose commit-set union is
order-convex, attaches their segments, invokes the kind-specific merge, and
publishes one new `(core,node)` pair for the union. Victim assertions are never
deleted:

```text
disjoint convex victim ranges
    -> merge their attached segments
    -> freeze one complete replacement node
    -> add one rollup assertion
```

`convex_union` is deliberately proof-shaped:

1. Expand and validate every victim's exact interval.
2. Form their disjoint set union `U`.
3. Derive `S' = minima(U)` and `E' = maxima(U)`.
4. Accept only when `R(S',E') == U`.

In a chain `A < B < C`, compacting `[A,A]` with `[C,C]` is rejected because
the hull `[A,C]` would add the missing commit `B`. Adjacent `[A,A] + [B,B]`
correctly becomes `[A,B]`.

Concurrent replicas may publish overlapping compactions or alternative
representations. Ordinary assertion-set union preserves them all; read-time
selection makes the useful physical plan. No durable frontier, level, sequence
number, victim list, fanout policy, snapshot replacement, or supersession
relation is part of the algebra.

## Trust and retention boundaries

The current typed rollup projection accepts assertions from the source
branch's own author under that branch name and recipe. That author certifies
that a node is a truthful derivation of its recipe and range. `thaw` proves
physical and recipe-visible invariants, but it does not generally recompute
provenance from source facts. Third-party builders would require a separate
delegation/admission design; no such policy is hidden in the cache format.

The hard core currently contains aligned commit handles. Strong reachability
therefore retains its boundary commits and reachable source ancestry. That is
redundant while the source branch is already strong, but it guarantees that the
residual fallback remains available.

Rollup caching requires no transaction over a bundle: partially downloaded
blobs are inert until the whole node thaws. Stores with garbage collection do
need a freshly derived ephemeral keep set for the currently desired node and
its exact typed components. This process policy must not be persisted as
grow-only demand.
