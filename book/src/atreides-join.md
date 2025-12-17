# The Atreides Family of Worst-case Optimal Join Algorithms

The query engine reasons about data by solving a set of constraints over
variables. Instead of constructing a traditional left-deep or bushy join plan,
it performs a guided depth-first search that binds one variable at a time. The
approach draws on the broader theory of worst-case optimal joins and lets us
navigate the search space directly rather than materialising intermediate
results.

## Before you read

This chapter builds on the constraint protocol described in
[Query Engine](query-engine.md) and assumes familiarity with the
identifier and trible layout conventions explained in
[Architecture](architecture.md#identifier-taxonomy) and
[Schemas](schemas.md#identifier-shapes-and-entropy). Those sections show why the
engine leans on high-entropy identifiers and six-way indexes when navigating the
search space.

## Constraints as the search frontier

Every constraint implements the [`Constraint`](crate::query::Constraint) trait,
which exposes four abilities that shape the search:

1. **`estimate`** – predicts how many results remain for a variable under the
   current partial binding.
2. **`propose`** – enumerates candidate values for a variable.
3. **`confirm`** – filters a set of candidates without re-enumerating them.
4. **`influence`** – reports which other variables need their estimates refreshed
   when this variable changes.

Traditional databases rely on a query planner to combine statistics into a join
plan. Atreides instead consults the constraints directly while it searches. Each
constraint can base its estimates on whatever structure it maintains—hash maps,
precomputed counts, or even constant values for predicates that admit at most
one match—so long as it can provide a quick upper bound. Whenever a binding
changes, the engine asks the influenced constraints for fresh estimates. Those
estimates are cached per variable and reused until another binding invalidates
them, keeping the guidance loop responsive as the search progresses.

Because the heuristics are derived entirely from the constraints themselves, we
do not need a separate query planner or multiple join implementations. Any
custom constraint can participate in the same search by providing sensible
estimates, proposal generation, confirmation, and influence tracking.

## A spectrum of Atreides variants

The Atreides "family" refers to the spectrum of heuristics a constraint can use
when implementing [`Constraint::estimate`](crate::query::Constraint). Each
variant exposes the same guided depth-first search, but with progressively
tighter cardinality guidance. In practice they all revisit their estimates when
bindings change; what differs is **what** quantity they approximate:

- **Row-count Join (Jessica)** estimates the remaining search volume for the
  *entire* constraint. If one variable is bound but two others are not, Jessica
  multiplies the candidate counts for the unbound pair (\|b\| × \|c\|) and
  reports that larger product. The number can wildly overshoot the next
  variable's frontier, yet it often tracks the overall work the constraint will
  perform.
- **Distinct-value Join (Paul)** narrows the focus to a single variable at a
  time. It returns the smallest proposal buffer the constraint could produce for
  any still-unbound variable, ignoring later confirmation filters. This is the
  behaviour exercised by [`Query::new`](crate::query::Query::new) today, which
  keeps the tightest candidate list on hand while the search walks forward.
- **Partial-binding Join (Ghanima)** goes further by measuring the size of the
  actual proposal the composite constraint can deliver for the current binding
  and chosen variable. For an `and` constraint this corresponds to the
  intersection of its children after they have applied their own filtering,
  revealing how many candidates truly survive the local checks.
- **Exact-result Join (Leto)** is an idealised limit where a constraint predicts
  how many of those proposed values extend all the way to full results once the
  remaining variables are also bound. Although no constraint currently achieves
  this omniscience, the interface supports it conceptually.

All four share the same implementation machinery; the difference lies in how
aggressively `estimate` compresses the constraint's knowledge. Even when only
partial information is available the search still functions, but better
estimates steer the traversal directly toward the surviving tuples.

Every constraint can decide which rung of this ladder it occupies. Simple
wrappers that only track total counts behave like Jessica, those that surface
their tightest per-variable proposals behave like Paul, and structures capable
of intersecting their children on the fly approach Ghanima's accuracy. The
engine does not need to know which variant it is running—`estimate` supplies
whatever fidelity the data structure can provide, and `influence` ensures that
higher quality estimates refresh when relevant bindings change.

## Guided depth-first search

When a query starts, [`Query::new`](crate::query::Query::new) collects the
initial estimates and influence sets, sorts the unbound variables so the
tightest constraints are considered first, and caches per-variable proposal
buffers that can be reused across backtracking steps. The engine then walks the
search space as follows:

1. Inspect the unbound variables.
2. Refresh the cached estimates for any variables whose constraints were
   influenced by the latest binding.
3. Pick the next variable to bind by sorting the unbound set on two criteria:
   - the base‑2 logarithm of the estimate (smaller estimates are tried first),
   - the number of other variables the constraints could influence (ties favour
     the most connected variable, which tends to prune the search faster).
4. Ask the relevant constraints to `propose` candidates for that variable.
   Composite constraints enumerate the tightest member and call `confirm` on the
   rest so that each candidate is checked without materialising cross
   products.
5. Push the candidates onto a stack and recurse until every variable is bound or
   the stack runs empty, in which case the engine backtracks.

Traditional databases rely on sorted indexes to make the above iteration
tractable. Atreides still performs random lookups when confirming each
candidate, but the cardinality hints let it enumerate the most selective
constraint sequentially and probe only a handful of values in the wider ones.
Because the search is depth-first, the memory footprint stays small and the
engine can stream results as soon as they are found.

Consider a query that relates `?person` to `?parent` and `?city`. The search
begins with all three variables unbound. If `?city` only has a handful of
possibilities, its estimate will be the smallest, so the engine binds `?city`
first. Each city candidate is checked against the parent and person constraints
before the search continues, quickly rejecting infeasible branches before the
higher-cardinality relationships are explored.

## Per-variable estimates in practice

Suppose we want to answer the following query:

```
(find [?person ?parent ?city]
  [?person :lives-in ?city]
  [?person :parent ?parent]
  [?parent :lives-in ?city])
```

There are three variables and three constraints. Every constraint can provide a
cardinality hint for each variable it touches, and the combined query records
the tightest estimate for each variable:

| Variable | Contributing constraints (individual estimates) | Stored estimate |
|----------|-------------------------------------------------|-----------------|
| `?person` | `?person :lives-in ?city` (12), `?person :parent ?parent` (40) | 12 |
| `?parent` | `?person :parent ?parent` (40), `?parent :lives-in ?city` (6) | 6 |
| `?city` | `?person :lives-in ?city` (12), `?parent :lives-in ?city` (6) | 6 |

The estimates are scoped to individual variables even when no single constraint
covers the whole tuple. The engine chooses the variable with the tightest bound,
`?parent`, and asks the constraints that mention it for proposals. Each
candidate parent immediately passes through the `?parent :lives-in ?city`
constraint, which usually narrows the possible cities to a handful. Those
cities, in turn, constrain the possible `?person` bindings. If a branch fails —
for example because no child of the selected parent lives in the same city — the
engine backtracks and tries the next parent. The smallest estimated constraints
therefore guide the search towards promising combinations and keep the
depth-first traversal from thrashing through unrelated values.

## Implementation notes

- During iteration the engine keeps a stack of bound variables, a `Binding`
  structure holding the active assignments, and a `touched_variables` set that
  marks which estimates need refreshing before the next decision point.
- Highly skewed data still behaves predictably: even if one attribute dominates
  the dataset, the other constraints continue to bound the search space tightly
  and prevent runaway exploration.
- The per-variable proposal buffers allow repeated proposals without
  reallocating, which is especially helpful when backtracking over large
  domains.

## Why worst-case optimal?

Worst-case optimal join algorithms guarantee that their running time never
exceeds the size of the output by more than a constant factor, even for
pathological inputs. The Atreides family retains this property because the
search always explores bindings in an order that honours the cardinality bounds
supplied by the constraints. As a result the engine remains robust under heavy
skew, sparse joins, and high-dimensional queries without requiring a bespoke
join plan for each case.

This combination of simple heuristics, incremental estimates, and a disciplined
search strategy keeps the implementation straightforward while delivering the
performance characteristics we need for real-world workloads.

## Deepen this topic

- The [Query Engine](query-engine.md) chapter walks through the search loop that
  consumes these heuristics.
- [Architecture](architecture.md#data-model) and
  [Schemas](schemas.md#identifier-shapes-and-entropy) explain how the trible
  layout and identifier choices keep estimates cheap and reliable.
