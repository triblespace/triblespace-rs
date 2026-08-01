# Incremental Queries

The query engine normally evaluates a pattern against a complete
`TribleSet`, recomputing every match from scratch. Applications that
ingest data continuously often only need to know which results are
introduced by new tribles. Tribles supports this with *semi-naive
evaluation*, a classic incremental query technique. Instead of running
the whole query again, we focus solely on the parts of the query that
can see the newly inserted facts and reuse the conclusions we already
derived from the base dataset.

## The Checkout pattern

`Workspace::checkout` returns a [`Checkout`] — a `TribleSet` paired with
the set of commits that produced it. That commit set acts as a continuation
token: pass it as the start of a range selector on the next checkout to
exclude already-seen commits.

```rust,ignore
// Initial load — full starts as a clone of the first checkout.
let mut changed = repo.pull(branch_identity)?.checkout(..)?;
let mut full = changed.clone();

loop {
    // Process new results. full already includes changed.
    for title in find!(title: String, pattern_changes!(&full, &changed, [
        { _?author @ literature::firstname: "Frank" },
        { _?book @ literature::author: _?author, literature::title: ?title }
    ])) {
        println!("new: {title}");
    }

    // Advance: exclude all commits we've already processed.
    changed = repo.pull(branch_identity)?.checkout(full.commits()..)?;
    full += &changed;
}
```

`Checkout` dereferences to `TribleSet`, so it works directly with
`find!`, `pattern!`, and `pattern_changes!`. The `full` accumulator is a
`Checkout` that grows monotonically. The `+=` operator merges both the
`TribleSet` facts and the `CommitSet`. The `changed` checkout carries the
commit set forward automatically.

This pattern avoids building shadow data models in Rust structs.
Query the `TribleSet` directly with `find!` — it has sub-microsecond
point lookups and single-digit microsecond joins.

## Delta evaluation

Given a full dataset and a set of changed tribles, the engine runs
the original query multiple times. Each run restricts a different triple
constraint to the changed set while the remaining constraints see the full set.
The union of these runs yields solutions supported by at least one changed
trible. A tuple emitted by an earlier invocation may legitimately recur when a
later delta adds another witness. The process is:

1. accumulate `changed` into `full` with `full += &changed`, which merges
   both the facts and the commit set,
2. for every triple in the query, evaluate a variant where that triple
   matches against `changed`,
3. union all per-triple results to obtain the incremental answers.

Because each variant touches only one triple from the changed set, the work
grows with the number of constraints and the size of the delta
rather than the size of the full dataset.

The variants are combined with `or!`, and a union deduplicates the candidate
values it proposes for each variable before the search descends. Two variants
that support the same assignment therefore contribute one candidate at every
level, so that assignment is enumerated once per query rather than once per
supporting variant.

That is the only deduplication involved. `find!` heads have bag semantics like
everywhere else: hidden variables still multiply the projected answer, so an
entity proved by several changed witnesses is emitted once per witness.
Collect into a set when you want each projected tuple once.

Nothing is remembered between invocations. A later delta can return the same
projected tuple again when a newly added fact supplies a new proof — that
reports support which is new in *that* delta, not a claim that the tuple was
absent from every earlier result. Applications that need global once-only
delivery should retain the tuples they have already consumed (in a set, say),
while applications that need distinct witness events should project the witness
identity explicitly.

## Monotonicity and CALM

Removed results are not tracked. Tribles follow the
[CALM principle](https://bloom-lang.net/calm/): a program whose outputs
are monotonic in its inputs needs no coordination. Updates simply add new
facts and previously derived conclusions remain valid. When conflicting
information arises, applications append fresh tribles describing their
preferred view instead of retracting old ones. Stores may forget obsolete
data, but semantically tribles are never deleted.

### Exclusive IDs and absence checks

Exclusive identifiers tighten the blast radius of non-monotonic logic
without abandoning CALM. Holding an `ExclusiveId` proves that no other
writer can add tribles for that entity, so checking for the *absence* of a
triple about that entity becomes stable: once you observe a missing
attribute, no concurrent peer will later introduce it. This permits
existence/absence queries in the narrow scope of entities you own while
keeping global queries monotonic.

Even with that safety net, prefer monotonic reads and writes when possible
because they compose cleanly across repositories. Absence checks should be
reserved for workflows where the `ExclusiveId` guarantees a closed world
for the entity — such as asserting a default value when none exists or
verifying invariants before emitting additional facts. Outside that
boundary, stick to append-only predicates so derived results remain valid
as new data arrives from other collaborators.

## Example

The `pattern_changes!` macro expresses these delta queries. It takes the
full `TribleSet` (which must include the changed tribles) and the changed
subset. The macro unions variants of the query where each triple is
constrained to the changed set, matching only results that involve at
least one new trible.

```rust,ignore
{{#include ../../examples/pattern_changes.rs:pattern_changes_example}}
```

The example commits Herbert and *Dune*, simulates an external update that
adds *Dune Messiah*, then uses the `Checkout` pattern to discover only the
newly added title. The multi-entity join links books to their author via
`_?author`, yet `pattern_changes!` returns only results where at least one
trible is new — *Dune* does not reappear. The first checkout loads the full
history; the second uses `changed.commits()..` to exclude
already-processed commits and fetch only new ones.

## Comparing history points

`Workspace::checkout` accepts [commit selectors](commit-selectors.md)
which can describe ranges in repository history. Checking out a range
like `a..b` walks the history from `b` back toward `a`, unioning the
contents of every commit that appears along the way but excluding commits
already returned by the `a` selector. When commits contain only the
tribles they introduce, that checkout matches exactly the fresh facts
added after `a`. Feeding that delta into `pattern_changes!` lets us ask,
"What new matches did commit `b` introduce over `a`?"

The `Checkout` type makes this ergonomic: `checkout(..)` returns both
the data and the commit set, so the next `checkout(commits()..)`
produces exactly the delta without manual bookkeeping.

## Trade-offs

- Applications must compute and supply the changed set; the engine does not
  track changes automatically.
- Queries must remain monotonic since deletions are ignored.
- Each triple incurs an extra variant, so highly selective constraints
  keep incremental evaluation efficient.
- Changed sets that grow unboundedly lose their advantage. Regularly
  draining or compacting the changeset keeps semi-naive evaluation
  responsive.
