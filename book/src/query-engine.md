# Query Engine

Queries describe the patterns you want to retrieve. The engine favors
predictable latency and skew resistance without a separately compiled query
plan. Every operator and data source implements the same
[`Constraint`](triblespace::core::query::Constraint) protocol, and the engine
consults those constraints while it searches. Binding order can therefore
adapt to the values already found instead of being fixed before evaluation.

The current protocol is **block-native**. Its unit of work is not necessarily
one partial binding, but a block of partial bindings that have the same set of
bound variables. The ordinary iterator uses a demand-adaptive DAG worklist;
the explicit [`Query::sequential`](triblespace::core::query::Query::sequential)
path speaks the same protocol with blocks of one row. This shared interface is
the important part of the design: a constraint has one implementation whether
its probes are issued one at a time, fused into a CPU loop, or dispatched to a
batch-oriented accelerator.

## Bindings as row blocks

A [`RowsView`](triblespace::core::query::RowsView) is a borrowed, row-major view
of partial bindings. Its `vars` slice names the columns and every row contains
one value for each of those variables. For example:

```text
vars = [person, city]

row 0 = [P1, Bremen]
row 1 = [P2, Arrakeen]
row 2 = [P3, Bremen]
```

All rows have bound the same variables, although their values differ. Column
order is not part of the protocol: constraints locate a variable with
`RowsView::col` rather than assuming a position. A view with no columns is the
seed block, represented as one virtual zero-width row. Consequently the empty
binding is an ordinary input to the protocol rather than a special engine case.

When the engine asks for candidates for another variable, the blocked form of
[`CandidateSink`](triblespace::core::query::CandidateSink) stores a ragged
matrix as `(row, value)` pairs:

```text
(0, E1), (0, E2), (2, E7)
```

Here row 0 has two extensions, row 1 dies, and row 2 has one. Pairs remain
grouped by row. A one-row caller instead uses the plain-values sink, where the
row index is statically zero and no tag is stored. Estimates follow the same
pattern through [`EstimateSink`](triblespace::core::query::EstimateSink): a
blocked caller receives one estimate per row, while the sequential caller
writes one scalar estimate directly into its cursor state.

## The constraint protocol

Six methods perform the query negotiation:

| Method | Responsibility |
|---|---|
| `variables` | Declare the variables the constraint touches. |
| `estimate` | Produce a candidate-count estimate for a variable and every input row. |
| `propose` | Enumerate candidate values for a variable and associate each value with its parent row. |
| `confirm` | Remove candidates that violate this constraint. |
| `satisfied` | Check the truth of a constraint whose relevant variables have become bound. |
| `influence` | Report which variables may need fresh estimates after another variable changes binding state. |

Four laws are load-bearing for correctness:

1. `propose` is always given an **empty** sink. A composite must preserve that
   ownership when delegating. In particular, each arm of a union proposes into
   its own empty buffer before the buffers are merged.
2. `confirm` may only filter its input. It must not append candidates, and it
   must preserve their row grouping.
3. `satisfied` may optimistically return `true` while one of the constraint's
   variables is unbound, but it **must be exact once all of them are bound**.
   This includes zero-variable constraints, which are fully bound at the seed.
4. Every row-taking verb is a **row homomorphism**. Splitting a block into
   non-empty consecutive sub-blocks, evaluating them independently, and
   concatenating the outputs (with candidate row tags remapped) must equal
   evaluating the whole block. In particular, estimates and proposals
   concatenate, confirmation is local to each candidate's row, and whole-block
   `satisfied` is the conjunction of the sub-block answers. Batched
   implementations may fuse physical work, but block-global top-k or first-row
   decisions are invalid. Diagnostics may observe call boundaries, but those
   observations must never feed back into protocol answers.

The third law is easy to mistake for an optimization hook, but it is a
soundness rule. An [`or!`](triblespace::core::prelude::or) constraint uses it to
discard alternatives contradicted by the current row before those alternatives
propose or confirm another variable. An optimistic answer for a fully bound,
false alternative could otherwise admit a row that no single alternative
satisfies. A fully constant pattern similarly has no variable through which the
search could discover failure, so [`Query::new`](triblespace::core::query::Query::new)
settles it with an exact `satisfied` call against the seed block.

`ignore!` removes its wildcard positions from the outward variable set. When a
union needs to gate such an arm after all surviving variables are bound, the
wrapper replays each visible variable as a singleton `confirm` call with that
variable temporarily omitted. This is the same filtering operation the
historical wrapper performed during search, including for confirm-only range
constraints. Hidden-only clauses remain inert and ignored names never become a
shared existential witness.

Constraints are otherwise stateless. Each method receives the current
`RowsView`; the engine does not notify constraints when it backtracks, chunks a
frontier, or processes work in a different order. This is what allows the same
constraint tree to run under both schedulers.

## One expansion step

An expansion still performs the familiar Atreides negotiation:

1. Estimate each unbound variable under the current partial bindings.
2. Choose the preferred next variable. In a multi-row block this decision is
   made per row, because different bound values can imply different
   cardinalities.
3. If rows prefer more than one variable, start with the nonempty exact-choice
   groups as active hubs. Repeatedly absorb one complete active source group
   into the compatible active target that yields the least total candidate
   estimate, then partition by the retained scheduled variable with a stable
   counting sort. Variables preferred by no row are not opened as new hubs.
4. For each group, ask the root constraint to propose that variable. An
   intersection chooses its tightest child per row to propose and runs the
   remaining children as whole-frontier confirmation passes. A union evaluates
   its still-satisfied alternatives independently and merges their candidates.
5. Extend the parent rows with the surviving `(row, value)` pairs. Rows without
   candidates disappear.

There is still no standalone join plan. The difference from the original
engine is that several sibling searches can negotiate and probe together.

## Sequential scheduler: a block of one

[`Query::sequential`](triblespace::core::query::Query::sequential) selects the
original depth-first behavior. It keeps a stack of bound variables and a
parallel row of values, plus a variable-to-column index for constant-time
lookup. At each depth it refreshes influenced estimates, chooses one variable,
calls `propose` with a one-row `RowsView` and a plain-values sink, then tries the
returned values one by one. Exhausting a proposal vector pops the cursor and
backtracks. Completing the cursor invokes the result conversion closure.

Thus the sequential path does not emulate batching by allocating tagged rows:
it is the zero-overhead scalar representation of the same protocol. It remains
valuable for low first-result latency, tiny candidate sets, and workloads where
there is no useful frontier to fuse.

## DAG worklist engine

The DAG engine replaces the recursive search stack with buckets keyed by the
**set of variables already bound**. Consider two rows that reach the same
state through different binding orders:

```text
             {p, a} ─────▶ {p, a, b}
            /                 ▲
          {p}                 │
            \                 │
             {p, b} ──────────┘
```

A multi-row `{p}` bucket can contain rows whose bound values make `a` the best
next variable and rows that prefer `b`. A tree-shaped evaluator would retain
two `{p, a, b}` frontiers, one for each history. The DAG evaluator stores
columns in canonical variable order and files both into the same `{p, a, b}`
bucket. The rows are merely co-located—each complete assignment still follows
exactly one route—but downstream constraints now receive a fatter batch.

One worklist pop performs the expansion described above: take a chunk of rows
from a bucket, estimate and partition them, propose and confirm once per group,
then file the extended rows under `bound ∪ {next}`. A full-bound bucket emits
rows instead. Parent work is
logically consumed into child buckets rather than retained as parallel search
trees; filing materializes each extended child row in canonical column order.

Reconvergence requires a scheduling rule. At full batch width, a bucket is
ready only when no live bucket has a strict subset of its bound-variable set.
Any future contributor must be such a subset because evaluation only adds
bindings. Waiting until those contributors drain lets routes actually meet;
strict deepest-first scheduling would normally consume a bucket immediately
after its first parent filed into it. The tradeoff is explicit: highly
reconvergent queries can retain a broader frontier and use more memory in
exchange for larger batches.

The ordinary [`Query`](triblespace::core::query::Query) iterator combines this
worklist with demand-adaptive chunking. Its width starts at one row and grows
geometrically whenever the consumer asks the engine to resume. Before the width
cap is reached, scheduling is strict deepest-first, preserving
sequential-class first-result behavior; after saturation, the readiness gate
turns on and the remaining computation enters the batch-harvesting regime. An
`exists!` or `take(1)` consumer can therefore discard the worklist after the
first match instead of paying for full enumeration.

Partition economics are decided by the split itself, not by chunk width. When
rows genuinely prefer more than one next variable, those exact-choice groups
become the leaves of an agglomerative merge hierarchy. A complete source group
may be absorbed by an active target variable `v` only when every source row's
binary estimate-magnitude regret fits the bit length of
`{v} ∪ (influence(v) ∩ unbound)`. Binding a variable that can refresh a larger
still-relevant downstream neighborhood can therefore justify a wider local cardinality bucket; an
isolated variable cannot. Rows whose preferred estimate is zero remain
compatible only with zero estimated work.

Among compatible directed absorptions the engine chooses the one with the
smallest resulting total candidate estimate, merges the groups, and repeats.
Compatibility is conjoined as groups merge, so one incompatible row keeps its
entire exact group separate. The planner returns the coarsest admissible level
of that greedy hierarchy; it does not globally score intermediate levels.
Exact grouping is the literal starting partition and remains the result when no
complete group fits. A one-row chunk is naturally uniform. There is no
independent width cutoff or fixed inflation factor. The choice affects order
and batching only: `propose` and `confirm` still determine the exact solutions.

For `R` rows and `V ≤ 128` unbound variables, planning is
`O(RV + V³)` time and the scheduler uses `O(RV + V²)` reusable scratch space
in total. The `RV` estimate matrix is already required by exact per-row
grouping; agglomeration adds `O(R + V²)` scratch beyond it. It builds the
row/group compatibility table once, then rescans the active directed edges for
at most `V - 1` absorptions.

Fully-bound rows remain in raw inline form until the consumer pulls them. The
worklist never stores projected result values, so a query's `Send`/`Sync`
properties do not depend on its output type and cloning a partially consumed
query snapshots its exact remaining raw state without requiring the result type
to implement `Clone`.

[`Query::solve_dag_lazy`](triblespace::core::query::Query::solve_dag_lazy)
exposes the same scheduler as a configurable iterator with explicit starting
width, growth, cap, and partition-policy controls. The eager/grouped probe
solvers pin grouping explicitly so they remain stable controls for the
agglomerative ordinary iterator.

[`Query::solve_dag`](triblespace::core::query::Query::solve_dag) is the eager,
saturated-width form. Fully drained schedulers produce the same result
**multiset**, but worklist scheduling may produce a different row order.

## Parallel execution

With the `parallel` feature, ordinary `IntoParallelIterator` consumption keeps
the established scalar DFS proposal splitter. This remains the CPU-oriented
default: inexpensive one-row probes can outperform the bookkeeping and wider
batches of a DAG worklist.

[`Query::into_par_dag_iter`](triblespace::core::query::Query::into_par_dag_iter)
is the explicit block-native alternative. It partitions a fresh query's affine
DAG frontier into at most one worklist shard per worker. Seed negotiation
proceeds until rows actually branch, so a deterministic prefix does not force
the explicit path back to its scalar cursor. Every shard retains block-native
estimation, per-row grouping, and route reconvergence among the rows it owns.
Cross-shard reconvergence is intentionally traded for CPU concurrency, and the
DAG starts at the configured row cap because full parallel enumeration is an
explicit throughput request. This path preserves batches for block-oriented
and accelerator-backed constraints even when scalar DFS is faster on CPU-only
workloads.

The optional `triblespace-gpu::WgpuSuccinctArchive` exercises that seam without
putting a device dependency in core. It wraps the canonical archive, keeps its
six Jerky wavelet matrices resident, and routes tagged `confirm` rank streams
through a device-neutral `RingBatchQuery`; estimates, proposals, prefix walks,
domain lookups, satisfaction checks, and scalar sinks remain on CPU. GPU
admission is per batch (8,192 rank probes by default), so affine sharding may
still create CPU fallbacks. This is intentional: forcing every Rayon shard to
emit synchronizing device work for every tiny rank batch is much slower than
either executor, while fat batches amortize fixed dispatch/readback costs and
use the device's rank throughput. `WgpuSuccinctArchive::stats` exposes
dispatches, fallbacks, probe totals, and batch extrema so backend/scheduler
economics are observable rather than hidden in a planner heuristic.
On the deterministic 1.77M-trible reconvergence probe (M4 Max, 16 Rayon
workers), each timed run kept 371 small rank batches on CPU and sent 54 batches
to Metal, reducing the controlled parallel-DAG median from 382 ms to 312 ms.
Forcing all 425 non-empty rank batches emitted by the shards to WGPU instead
took 775 ms, demonstrating that the admission boundary is part of the
algorithm rather than a backend detail.

A partially consumed ordinary DAG query converted through `into_par_iter()` is
drained as one parallel leaf so its exact remaining state cannot be restarted.
The explicit DAG entry point requires a fresh query. With one Rayon worker it
has a zero split budget; with `N` workers it permits at most `N - 1` splits. In
every case the result guarantee is multiset equality, not iteration order.

Both parallel paths clone the constraint tree and result postprocessor per
shard. Code that needs aggregate observations across clones should use shared
synchronization such as `Arc<AtomicU64>`; clone-local interior state is not a
global invocation counter. The row-homomorphism law above is what permits the
engine to change chunk and shard boundaries without changing results.

## Queries as Schemas

You might notice that trible.space does not define a global ontology or schema
beyond associating attributes with a
[`InlineEncoding`](triblespace::core::inline::InlineEncoding) or
[`BlobEncoding`](triblespace::core::prelude::BlobEncoding). This is deliberate. The semantic web
taught us that per-value typing, while desirable, was awkward in RDF: literal
datatypes are optional, custom types need globally scoped IRIs and there is no
enforcement, so most data degenerates into untyped strings. Trying to regain
structure through global ontologies and class hierarchies made schemas rigid
and reasoning computationally infeasible. Real-world data often arrives with
missing, duplicate or additional fields, which clashes with these global,
class-based constraints.

Our approach is to be sympathetic to edge cases and have the system deal only
with the data it declares capable of handling. These application-specific
schema declarations are exactly the shapes and constraints expressed by our
queries[^1]. Data not conforming to these queries is simply ignored by
definition, as a query only returns data satisfying its constraints.[^2]

## Join Strategy

The query engine uses the Atreides family of worst-case optimal join
algorithms. These algorithms leverage the same cardinality estimates surfaced
through `Constraint::estimate` to guide variable choice over partial bindings,
providing skew-resistant and predictable performance. The sequential scheduler
explores those choices depth-first; the DAG scheduler begins from the same
per-row choices, may softly coalesce compatible complete preference groups, and
files the results through its worklist. Because both refresh estimates during
evaluation, binding order adapts whenever a constraint updates its influence
set—there is no separate planning artifact to maintain.
For a detailed discussion, see the [Atreides Join](atreides-join.md) chapter.

## Query Languages

Instead of a single query language, the engine exposes small composable
constraints that combine with logical operators such as `and` and `or`. These
constraints are simple yet flexible, enabling a wide variety of operators while
still allowing the engine to explore the search space efficiently.

The query engine and data model are flexible enough to support many query
styles, including graph, relational and document-oriented queries. Constraints
may originate from the database itself (such as attribute lookups), from custom
application logic, or from entirely external sources.

For example, the [`pattern!`](triblespace::core::macros::pattern!) and
[`entity!`](triblespace::core::macros::entity!) macros—available at the crate root and re-exported
via [`triblespace::prelude`](triblespace::prelude) (for instance with
`use triblespace::prelude::*;`)—generate constraints for a given trible pattern in
a query-by-example style reminiscent of SPARQL or GraphQL but tailored to a
document-graph data model. It would also be possible to layer a property-graph
language like Cypher or a relational language like Datalog on top of the
engine.[^3]

```rust
use std::collections::HashSet;

use triblespace::core::examples::literature;
use triblespace::core::query::ContainsConstraint;
use triblespace::prelude::*;
use triblespace::prelude::inlineencodings::ShortString;

fn main() {
    let mut kb = TribleSet::new();

    let author = ufoid();
    let book = ufoid();

    kb += entity! { &author @
        literature::firstname: "Frank",
        literature::lastname: "Herbert",
    };
    kb += entity! { &book @
        literature::author: &author,
        literature::title: "Dune",
    };

    let mut allowed = HashSet::<Inline<ShortString>>::new();
    allowed.insert("Frank".to_inline());

    let results: Vec<_> = find!((title: Inline<_>, firstname: Inline<_>),
        and!(
            allowed.has(firstname),
            pattern!(&kb, [{
                _?person @
                    literature::firstname: ?firstname,
                    literature::lastname: "Herbert",
            }, {
                literature::author: _?person,
                literature::title: ?title,
            }])
        )
    )
    .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "Dune".to_inline());
}
```

The snippet above demonstrates how typed attribute constraints, user-defined
predicates (the `HashSet::has` filter), and reusable namespaces can mix
seamlessly within a single query.

Great care has been taken to ensure that query languages with different styles
and semantics can coexist and even be mixed with other languages and data models
within the same query. For practical examples of the current facilities, see the
[Query Language](query-language.md) chapter.

[^1]: Note that this query-schema isomorphism isn't necessarily true in all
databases or query languages, e.g., it does not hold for SQL.
[^2]: In RDF terminology: We challenge the classical A-Box & T-Box dichotomy by
replacing the T-Box with a "Q-Box", which is descriptive and open rather than
prescriptive and closed. This Q-Box naturally evolves with new and changing
requirements, contexts and applications.
[^3]: SQL would be a bit more challenging, as it is surprisingly imperative
with its explicit JOINs and ORDER BYs, and its lack of a clear declarative
semantics. This makes it harder to implement on top of a constraint-based query
engine tailored towards a more declarative and functional style.
