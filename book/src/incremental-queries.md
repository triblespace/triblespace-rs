# Incremental Queries

The query engine normally evaluates a pattern against a complete
`TribleSet`, recomputing every match from scratch. Applications that
ingest data continuously often only need to know which results are
introduced by new tribles. Tribles supports this with *semi‑naive
evaluation*, a classic incremental query technique. Instead of running
the whole query again, we focus solely on the parts of the query that
can see the newly inserted facts and reuse the conclusions we already
derived from the base dataset.

## Delta evaluation

Given a base dataset and a set of newly inserted tribles, the engine runs
the original query multiple times. Each run restricts a different triple
constraint to the delta while the remaining constraints see the full set.
The union of these runs yields exactly the new solutions. The process is:

1. compute a `delta` `TribleSet` containing only the inserted tribles,
2. for every triple in the query, evaluate a variant where that triple
   matches against `delta`,
3. union all per‑triple results to obtain the incremental answers.

Because each variant touches only one triple from the delta, the work
grows with the number of constraints and the size of the delta set
rather than the size of the full dataset.

In practice the base dataset is often cached or already available to the
application. Maintaining a delta set alongside it lets the engine
quickly answer “what changed?” without re-deriving prior results. If the
delta is empty the engine can skip evaluation entirely, making idle
updates effectively free.

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
for the entity—such as asserting a default value when none exists or
verifying invariants before emitting additional facts. Outside that
boundary, stick to append-only predicates so derived results remain valid
as new data arrives from other collaborators.

## Example

The `pattern_changes!` macro expresses these delta queries. It behaves
like `pattern!` but takes the current `TribleSet` and a precomputed
changeset. The macro unions variants of the query where each triple is
constrained to that changeset, matching only the newly inserted tribles.
It keeps the incremental flow compatible with the familiar `find!`
interface, so callers can upgrade existing queries without changing how
they collect results.

```rust
{{#include ../../examples/pattern_changes.rs:pattern_changes_example}}
```

The example stages an initial commit, records a follow-up commit, and
then compares their checkouts. Feeding the latest checkout alongside the
difference between both states allows `pattern_changes!` to report just
the additional solutions contributed by the second commit. This mirrors
how an application can react to a stream of incoming events: reuse the
same query, but swap in a fresh delta set each time new data arrives.

Delta maintenance always comes down to set algebra. `pattern_changes!`
cares only that you hand it a `TribleSet` containing the fresh facts,
and every convenient workflow for producing that set leans on the same
few operations:

- take two snapshots and `difference` them to discover what was added;
- `union` the new facts into whatever baseline you keep cached;
- `intersect` a candidate subset when you need to focus a change
  further.

Workspaces showcase this directly. Each checkout materializes a
`TribleSet`, so comparing two history points is just another snapshot
diff: take the newer checkout, `difference` it against the older one to
obtain the delta, and hand both sets to `pattern_changes!`. That matches
the local buffering story as well. Keep a baseline `TribleSet` for the
current state, accumulate incoming facts in a staging set, and union the
staging set with the baseline to produce the updated snapshot you pass as
the first argument. The delta comes from `difference(&updated, &old)` or
from the staging set itself when you only stage fresh facts. Reusing the
same set helpers keeps the bookkeeping short, avoids custom mirrors of
the data, and stays efficient no matter where the updates originate.

## Comparing history points

`Workspace::checkout` accepts [commit selectors](commit-selectors.md)
which can describe ranges in repository history. Checking out a range
like `a..b` walks the history from `b` back toward `a`, unioning the
contents of every commit that appears along the way but excluding commits
already returned by the `a` selector. When commits contain only the
tribles they introduce, that checkout matches exactly the fresh facts
added after `a`. Feeding that delta into `pattern_changes!` lets us ask,
“What new matches did commit `b` introduce over `a`?”

## Trade‑offs

- Applications must compute and supply the delta set; the engine does not
  track changes automatically.
- Queries must remain monotonic since deletions are ignored.
- Each triple incurs an extra variant, so highly selective constraints
  keep incremental evaluation efficient.
- Delta sets that grow unboundedly lose their advantage. Regularly
  draining or compacting the changeset keeps semi-naive evaluation
  responsive.
