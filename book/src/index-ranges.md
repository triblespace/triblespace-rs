# Range-Native Derived Indexes

Derived indexes are replaceable views over repository history. Their identity
should therefore describe *which source commits they cover*, while the chosen
index representation remains an open attribute on that source range.

The `repo::index_range` module supplies this artifact-neutral foundation. It
does not assign a generic `kind` tag or erase every artifact into one `blob`
field. A consumer instead defines typed attributes such as a Succinct archive,
Rank9 accelerator, BM25 segment, or HNSW graph and attaches those facts to a
stable range entity.

## Inclusive DAG frontiers

A commit history is a DAG, so a source region cannot generally be represented
by one linear start and end cursor. Each range has two repeated attributes:

- `commit_start*`: the minimal included commits;
- `commit_end*`: the maximal included commits.

Both frontiers are nonempty antichains and both are inclusive. A one-commit
leaf—including a genesis commit—is `[C,C]`. There is no null or exclusive
cursor sentinel.

For start frontier `S` and end frontier `E`, the range denotes the union of
closed commit-poset intervals:

```text
R(S,E) = { x | some s in S and e in E satisfy s <= x <= e }
```

The frontiers must be exact: `S` must equal the minima of `R(S,E)` and `E`
must equal its maxima. This rejects comparable values within one frontier and
disconnected boundaries.

A diamond illustrates why the values are repeated:

```text
       A
      / \
     G   M
      \ /
       B
```

- the sibling region `{A,B}` has `start={A,B}, end={A,B}`;
- the full diamond has `start={G}, end={M}`;
- the merge commit alone is the leaf `start={M}, end={M}` and does not include
  either parent.

## Stable range identity and open facts

The `RangeRecord` identity is the intrinsic core
`(index_recipe, commit_start*, commit_end*)`. A raw Succinct archive and the
Rank9 accelerator built specifically for it can therefore share one recipe
slot and lifecycle, while BM25, HNSW, or even another configuration over the
same commits receives a different recipe and cannot collapse into that entity.
Artifact handles themselves never participate in the id.

`RangeRecord` retains every fact whose subject is its real entity id, including
attributes unknown to the current binary, and refreshes only the intrinsic
recipe-plus-frontier core when it is serialized. `select_range_record_facts`
is the preferred carry-forward primitive because it copies selected entity
facts verbatim without parsing or reconstruction.

Independent typed maintenance uses `replace_range_attributes`: it removes only
the selected `(entity, typed attribute)` facts and preserves all co-located and
unknown attributes. The recipe/range core remains even with zero typed handles;
that is the canonical completed-empty projection. `replace_range_records`
removes every fact under a retired entity and is used when compacting the
complete recipe/range slot and all artifacts owned by it.

## Exact compaction

Compaction may merge ranges only when their logical commit sets are disjoint
and their union is order-convex. The algorithm is deliberately proof-shaped:

1. Expand and validate every victim's exact interval.
2. Form their set union `U`, rejecting overlap.
3. Derive `S' = minima(U)` and `E' = maxima(U)`.
4. Accept only if `R(S',E') == U`.

The equality check is the correctness gate. In a chain `A < B < C`, compacting
`[A,A]` with `[C,C]` is rejected because the candidate hull `[A,C]` would add
the missing commit `B`. Adjacent `[A,A] + [B,B]` correctly becomes `[A,B]`.

The base-FANOUT LSM carry normally merges consecutive blocks of the global
parents-first commit order, so its victim unions are convex. Imported or
manually assembled manifests receive no such assumption and must fail closed
when the equality does not hold.

## Whole-cover audit

For each typed artifact attribute independently, live ranges across *all* LSM
levels form one partition:

```text
pairwise-disjoint union(ranges) == ancestors(branch HEAD)
```

`validate_exact_cover` performs this audit and catches interior holes,
overlapping live ranges, and artifacts from unreachable forks. An empty branch
requires zero ranges. Different artifact types may use different partitions;
overlap across those independent typed covers is expected.

Filtered or contentless commits still belong to a cover. Their canonical empty
range record contains the recipe and boundaries with zero typed handles;
omitting the record could not distinguish “certified empty” from “missing or
stale.”

## Commit batches

`range_for_commit_set` derives the inclusive boundaries of a nonempty convex
commit batch. For repository push hooks, its input is exactly the newly
reachable parents-first commit set. The prior `base_head` is an exclusion
cursor, not an inclusive range start.

For example, after a conflict where winner `A` and losing sibling `B` are
joined by merge `M`, retrying from base `A` introduces `{B,M}` and produces
the range `[B,M]`. Before extending an existing cover, the integration must
also prove that the old base is an ancestor of the new head. Rewinds and
unrelated head replacements require a rebuild; certifying them incrementally
would retain commits outside the new head's history.
