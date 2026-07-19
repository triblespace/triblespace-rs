# Query Engine

Queries describe the patterns you want to retrieve. The engine favors
predictable latency and skew resistance without a separately compiled query
plan. Every operator and data source implements the same
[`Constraint`](triblespace::core::query::Constraint) protocol, and the engine
consults those constraints while it searches. Binding order can therefore
adapt to the values already found instead of being fixed before evaluation.

The current protocol is **block-native**. Its unit of work is not necessarily
one partial binding, but a block of partial bindings that have the same set of
bound variables. Every live serial ordinary
iterator uses the canonical residual-state worklist. The bound-variable-set
DAG and [`Query::sequential`](triblespace::core::query::Query::sequential)
remain explicit controls; the sequential path speaks the same protocol with
blocks of one row. This shared interface is the important part of the design:
a constraint has one implementation whether its probes are issued one at a
time, fused into a CPU loop, or dispatched to a batch-oriented accelerator.

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

Constraints are otherwise stateless. Each method receives the current
`RowsView`; the engine does not notify constraints when it backtracks, chunks a
frontier, or processes work in a different order. This is what allows the same
constraint tree to run under every scheduler.

## One expansion step

An expansion still performs the familiar Atreides negotiation:

1. Estimate each unbound variable under the current partial bindings.
2. Choose the preferred next variable. In a multi-row block this decision is
   made per row, because different bound values can imply different
   cardinalities.
3. Stable-partition the rows by their exact preferred variable. This preserves
   each row's selected occurrence bag while still batching rows whose preferred
   variable agrees; no row is reassigned to an estimate-similar variable.
4. For each group, propose that variable. The DAG asks the root constraint; an
   intersection chooses its tightest child per row and runs the remaining
   children as whole-frontier confirmation passes. The residual engine makes
   the same proposer and confirmer choices explicit worklist actions. A union
   remains an opaque leaf that evaluates its still-satisfied alternatives
   independently and merges their candidates.
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

## Canonical residual-state engine

The residual engine keys a bucket by its **remaining computation**, not merely
by the bindings or the route that produced it. Its conservative explicit
controls recursively flatten the maximal associative AND region exposed at the
root into deterministic preorder leaf occurrences. Union, regular-path, and
custom constraints remain opaque leaves unless a capability explicitly exposes
more structure, so lowering never crosses an undeclared semantic boundary.

Every live ordinary root runs as one finite formula after variable selection.
Exposed AND/OR progress then becomes canonical formula state, and eligible
cyclic regular paths run through the delta submachine. `ProgramScope` is an
orthogonal three-level policy: `Disabled` admits no typed Programs,
`Production` admits production-qualified routes, and `All` also admits
explicit routes. A structurally absent route may still use older transition
hooks. A route deferred by policy instead uses the stable ordinary
`Constraint` action; it never falls through to a legacy pager or strengthens
an ordinary proposal receipt. The formula and Program scope chains therefore
form nine scheduler-independent lowering combinations.

Each canonical descriptor includes the bound-variable schema and one of four
phases:

- `Ready` jointly chooses a row's next variable and exact proposing leaf.
- `Propose` invokes one uniform proposer over an assembled parent-row bucket.
- `Candidate` chooses the next unchecked relevant confirmer.
- `Confirm` invokes one uniform confirmer over complete parent candidate
  groups, or over candidate pages once every remaining confirmer declares that
  operation page-local.

Planning phases only estimate, partition, and file work; protocol calls happen
in the explicit action phases. The checked-leaf set is canonical, so histories
that applied the same constraints in different orders can append to the same
future state before its remaining work runs. Candidate payloads remain
occurrence bags while a whole-group action can still distinguish them. At the
first state where a fully checked binding may commit or the remaining
confirmers allow independent candidate pages, the engine reverse-stably admits
one `(parent row, value)` occurrence. This preserves tail scheduling order and
keeps equal values under different affine parents independent. Finite Formula
AND continuations use the same boundary; Formula OR retains its own private
ordered-set reducer, and segmented affine payloads cross through a bounded
engine admission phase rather than synchronous materialization. The terminal
projection remains the universal final SET guard across hidden witnesses and
routes.

Lazy residual execution begins with actionable width one. A surviving action
keeps its newly filed continuation hot, allowing a successful path to descend
and emit before cold siblings are evaluated. Dead actions and terminal rows
grow the desired width geometrically; once no hot continuation can run, an
occupancy/readiness policy harvests wider batches. This gives the state machine
the same low-latency-to-throughput ramp as the DAG without requiring a complete
intersection to run eagerly for one binding.

Regular-path product states apply that demand inside a node as well as across
nodes. Positive, inverse, and negated attribute transitions expose an ordered
frontier whose cursor is `(automaton branch, last value)`. A width-one pull can
therefore inspect one distinct destination of a high-degree node, file both its
affine expansion continuation and any novel child, and descend toward a result
without first materializing the complete adjacency. Branch-qualified cursors
keep distinct NFA futures separate even when they produce the same graph value.
For `!p`, EVA pages distinct forward destinations and VEA pages distinct
inverse subjects. The destination's attribute suffix then answers `exists a !=
p`; because the current path algebra excludes one attribute, the exact inner
test needs at most its first attribute and one strict successor. Destinations
reachable only through `p` count against demand but produce no child. This
keeps mixed positive/negated states under one global width without enlarging
the activation-private cursor or relying on fixpoint deduplication. A
transition page that produces no novel child, accepted endpoint, or stable
continuation contributes negative feedback, so a rejected prefix grows from
one to two to four destinations instead of remaining a width-one serial scan.
An accepting initial product root is settled one step earlier. Activation
creation records its endpoint in the same distinct accepted set used by later
transition witnesses and returns a one-shot seed-effect receipt to the delta
scheduler. A streaming proposal or fully-bound Boolean Support reducer files
that receipt into the stable machine immediately, while the root's affine
traversal credit remains live for non-epsilon paths. Grouped confirmation and
non-linear formula proposal retain their existing quiescence barriers: seed
acceptance is private reducer state there, not an illegally streamed result.
This mechanism is generic to `ResidualDeltaOutput::accepted`, not an RPQ
branch in the scheduler. It preserves NODES(G) gating, same-variable paths,
duplicate outer parent bags, and clone/drop remainders. Seed publication
consumes neither transition width nor a transition-page statistic, and the
first later expansion cannot replay it. Conversely, an independently dead
source or transition page still supplies geometric negative feedback even if
the activation published an earlier seed effect.

Paged product nodes under the same structural transition operator cross one
block-native cohort seam. The batch carries row-aligned nodes, affine cursors,
and ragged limits whose sum is the current global width; successors return with
input-node tags. A constraint may page some rows while leaving other `Start`
rows to the existing eager block expansion, so one negated fallback does not
erase bounded positive work. The default lowers the cohort to scalar page
calls, while storage or accelerator constraints can fuse it without changing
canonical state or producer-credit semantics.

Final-variable streaming activations use a second physical policy on that same
seam. A directed hot continuation still advances exactly one activation: its
source pager receives global search width `S`, while its transition pager
receives the activation-local sparse quantum `t_a`, capped by `S`. Cold global
harvesting may instead cohort compatible terminal activations. Source rows
share one budget `B=S`; transition rows share
`B=min(S, sum_a t_a)`, with ragged task limits that never spend more than one
activation's `t_a` on its behalf. The backend call is shared, but feedback is
not: an activation that publishes resets to one independently of a sibling
whose live transition miss doubles its own quantum. Source misses leave every
transition quantum unchanged. A negative transition cohort reaches outer
search-width growth only after it saturates `S` and leaves terminal work live.

The ordinary [`Query`](triblespace::core::query::Query) uses this engine whenever
exact seed settlement leaves a live search. Opaque roots, one-leaf ANDs,
disjoint conjunctions, finite Union roots, RPQ roots, and live zero-variable
truths therefore all exercise the same residual substrate. A seed-rejected
query starts no worklist at all. Production lowering keeps finite logical
composites as fused constraint kernels inside that substrate while enabling
production-qualified typed Programs for RPQs and other heterogeneous actions.
Explicit page-producing routes, including UnionArchive Propose and Confirm,
require `ProgramScope::All` (provided by `ResidualLowering::FULL`). The explicit
lazy DAG remains the comparison path.

[`Query::residual_state_scheduler`](triblespace::core::query::Query::residual_state_scheduler)
selects the residual cursor for any root while preserving the query's chosen
lowering (`HYBRID` by default; `Query::residual_lowering` may change it).
`solve_residual_state_lazy` is the separate conservative-lowering capability
control and exposes its width policy;
`solve_residual_state` is the eager saturated form, and
`solve_residual_state_profiled` reports state, merge, action, and batch
measurements. Fully drained variants preserve the distinct raw projected-row
set, but may change result order.

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

The ordinary [`Query`](triblespace::core::query::Query) uses residual states for
every live seed;
[`Query::lazy_dag_scheduler`](triblespace::core::query::Query::lazy_dag_scheduler)
selects this worklist explicitly for comparison. Demand-adaptive chunk width
starts at one row and grows geometrically whenever the consumer asks the engine
to resume. Before
the width cap is reached, scheduling is strict deepest-first, preserving
sequential-class first-result behavior; after saturation, the readiness gate
turns on and the remaining computation enters the batch-harvesting regime. An
`exists!` or `take(1)` consumer can therefore discard the worklist after the
first match instead of paying for full enumeration.

Variable grouping preserves the exact adaptive action selected for each row.
The selected variable and proposer occurrence own that row's candidate action;
row-homomorphic execution only proves that identical actions may be chunked and
rejoined. It does not make different variables commute. The scalar scheduler
reverse-stably SET-admits a proposal's values before descending or splitting
the cursor. The DAG partitions by exact preferred variable, delegates
row-local proposer choice to the root constraint's block-native `propose`, and
then SET-admits `(parent row, value)` before filing children. Consequently an
intra-parent duplicate disappears while equal values under distinct parents
remain independent. The residual engine cohorts explicit
`(variable, proposer occurrence)` actions and still carries their occurrence
payloads. Neither path moves a row to an estimate-compatible action. Exact
ordering-key ties choose the lower variable ID in every planner. Chunk width
and pop order remain physical choices.

For `R` rows and `V ≤ 128` unbound variables, this planning is `O(RV)` time and
uses `O(RV + V)` reusable scratch space, dominated by the per-row estimate
matrix and stable counting partition.

Both block-native engines keep fully-bound rows in raw inline form until the
consumer pulls them. Neither worklist stores projected result values, so a
query's `Send`/`Sync` properties do not depend on its output type and cloning a
partially consumed query snapshots its exact remaining raw state and claimed
projection keys without requiring the result type to implement `Clone`.

[`Query::solve_dag_lazy`](triblespace::core::query::Query::solve_dag_lazy)
exposes the same scheduler as a configurable iterator with explicit starting
width, growth, and cap controls. Exact per-row action grouping is an invariant,
not a selectable physical policy.

[`Query::solve_dag`](triblespace::core::query::Query::solve_dag) is the eager,
saturated-width form. Fully drained schedulers produce the same result
**set of raw projected rows**, but worklist scheduling may produce a different
row order.

## Terminal projection and SET identity

Every semantic action admits a SET before publishing successors. Scalar and
DAG proposal actions remove duplicate values for each affine parent, and
residual sources and transitions perform the same admission at their stable
boundary. Internal probes may still carry occurrence bags before that boundary,
but every complete raw binding is therefore unique when it reaches projection.

For a strict `find!` head, projection is not injective: complete bindings that
differ only in hidden witnesses can have the same public identity. The terminal
projection gate derives an ordered key from the head's raw inline bytes and
claims it before running `TryFromInline` conversion or user mapper code. A
second binding with that projected key is discarded, even when its hidden
witness or route through an `or!` differs. A complete head is injective over
the already-unique bindings, so it elides the terminal claim table, key
allocation, and parallel claim mutex.

This ordering gives projection ordinary relational SET semantics while keeping
conversion outside the relational identity. Two distinct raw keys may convert
to Rust values that compare equal and are still emitted separately. In the
other direction, a failed conversion, mapper returning `None`, or mapper panic
consumes a strict head's raw key; another witness cannot retry user code for
that key. The empty head has one possible public key, so
`find!((), constraint)` emits at most one unit value. When the constraint has
variables, claiming that strict singleton key exhausts the public projection:
the next pull stops before scheduler work, while `None` stops the claiming pull
and a caught mapper panic leaves the next pull immediately exhausted. For a
zero-variable query the empty head is also the complete head; its sole semantic
seed provides the same at-most-once behavior without a claim table.

The `find!` macro supplies its explicit ordered head. Direct `Query::new`
construction uses every variable in the constraint as its conservative head,
so it removes only byte-identical complete bindings. There is no public bag
mode.

Cloning a serial iterator copies both its remaining raw cursor and, for a
strict head, its claimed keys into an independent snapshot. Rayon strict-head
sibling shards instead share one run-owned claim domain, ensuring that
duplicates discovered by different workers are still emitted once. Full heads
carry no claim state in either execution mode.

## Parallel execution

With the `parallel` feature, ordinary `IntoParallelIterator` consumption keeps
the established scalar DFS proposal splitter. This remains the CPU-oriented
default: inexpensive one-row probes can outperform the bookkeeping and wider
batches of either worklist. An unstarted ordinary query uses this scalar path
even though its serial default uses residual states.

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

[`Query::into_par_residual_state_iter`](triblespace::core::query::Query::into_par_residual_state_iter)
is the corresponding explicit residual path. It advances one exact state
machine until an affine frontier can be divided and creates at most one shard
per worker. Rows, complete candidate-parent groups, and candidates whose entire
remaining confirmation suffix is page-local are valid shard atoms; a
whole-group confirmer keeps each parent's ragged candidate sequence intact.
Every shard retains canonical state merging locally. As with the DAG splitter,
cross-shard reconvergence is traded for concurrency, state is moved rather
than duplicated, and the constraint/postprocessor pair is cloned only when a
real sibling shard is created. This entry point preserves the query's selected
residual lowering: fresh queries use hybrid lowering, which keeps formula
kernels fused and enables transition Programs, while an explicit
`Query::residual_lowering` override remains in force.

### Opt-in residual action observation

A configured residual iterator can be wrapped with
[`ResidualStateIter::shadow`](triblespace::core::query::residual::ResidualStateIter::shadow)
and a fresh
[`ResidualShadowEpoch`](triblespace::core::query::residual::ResidualShadowEpoch). The
wrapper observes only concrete `Propose` and `Confirm` dispatches, including
actions performed while a parallel producer negotiates its first splittable
frontier. It records the exact leaf occurrence, variable, bound schema, input
geometry, wall time, immediate survival or death, and any executor-local
samples. The ordinary residual iterator and executor contain no observer
field, clock read, thread-local lookup, observer allocation, or observer
option branch.

Action event numbers and leaf occurrences are local to one claimed epoch;
neither exposes the machine's private interner `StateId`. Serial exhaustion
and a fully drained Rayon drive close the epoch. `Closed` is a proof state: the
affine frontier was exhausted and every begun action has an ordinary
completion. Normal close is therefore private to the iterator/drive that owns
that frontier; a live or aborted action forces `Invalidated`, and the two
terminal states never transition into one another. Dropping an unfinished
serial wrapper, a panic anywhere in one pull (planning, action, or projection),
a parallel short circuit, or a parallel unwind invalidates it immediately,
even when the caller catches the unwind and retains the wrapper. A subsequent
pull is rejected.

Each Rayon producer carries its own armed abandonment guard. The guard is
disarmed only after that producer observes exact exhaustion (`next() == None`),
so a consumer that is already full, an abandoned split side, and cancellation
without a fold all invalidate the top-level drive. Converting a serial wrapper
that already proved exhaustion yields an empty Rayon iterator and preserves
`Closed`.

An event is registered first, then its thread-local correlation scope is
installed. Its public dispatch offset is published through the epoch's
snapshot gate; after that gate and every observer lock are released, a
separate private execution timer begins immediately before the unchanged task
executor. Successful execution captures and records that duration before the
correlation scope is removed, excluding registration, snapshot contention,
scope setup/teardown, and outcome mapping from action wall time. A snapshot is
a consistent copy at its terminal/open state. During the narrow
registration-to-dispatch window, the non-optional `started` field temporarily
uses the registration offset; dispatch replaces it with the actual offset.
An event admitted while the epoch was open may still publish and complete
after explicit invalidation: observation never cancels engine work, and its
completion is retained as stale. Samples filed after a terminal transition
likewise remain attached to their original event and are marked stale.

[`current_residual_action`](triblespace::core::query::residual::current_residual_action)
provides a stack-scoped correlation capability during a leaf call, so nested
observed queries restore the outer action on return. An asynchronous backend
must clone and carry that capability explicitly to another thread; ambient
thread-local state is not propagated. Observations are diagnostics only: they
must never feed estimates, protocol answers, state identity, action ordering,
or scheduling decisions in the execution they observe.

The optional `triblespace-gpu::WgpuSuccinctArchive` exercises that seam without
putting a device dependency in core. It wraps the canonical archive, keeps its
six Jerky wavelet matrices resident, and routes every nonempty `confirm` rank
stream through a device-neutral `RingBatchQuery`; estimates, proposals, prefix
walks, domain lookups, and satisfaction checks remain on CPU. Candidate storage
is not an execution capability: both a one-parent plain-values stream and a
multi-parent tagged stream reach the backend. GPU admission is per batch (8,192
rank probes by default), so either representation may still fall back to CPU.
This is intentional: forcing every tiny rank batch to emit synchronizing device
work is much slower than either executor, while fat batches amortize fixed
dispatch/readback costs and use the device's rank throughput.
`WgpuSuccinctArchive::stats` exposes
dispatches, fallbacks, probe totals, and batch extrema so backend/scheduler
economics are observable rather than hidden in a planner heuristic.
`WgpuSuccinctArchive::observe_residual_actions()` returns a borrowing,
non-`Deref` adapter for the additional opt-in executor bridge. Bind that adapter
before pattern construction so the GAT-produced constraint can borrow it for
the full query lifetime. The direct `WgpuSuccinctArchive` pattern path remains
structurally unobserved and performs no action-correlation lookup, clock read,
or sample work.

The adapter samples every nonempty Succinct confirmation rank stream offered to
the backend, whether its candidates use the plain-values or tagged
representation. It does not reinterpret all CPU work inside the action as
archive work, and planning, proposal, domain lookup, and satisfaction remain
unsampled. An empty rank stream records nothing; a nonempty call outside a
current observed action executes normally without a sample. Exact work is
`positions.len()` in `rank-probes`. Threshold fallbacks
are labelled `cpu` / `wavelet-rank/threshold-fallback`, while admitted device
calls are labelled `wgpu` / `wavelet-rank/gpu-round-trip`. These labels come
from the private per-call route that actually executes rather than from the
racy aggregate counters. Executor wall brackets only the selected rank backend;
route selection, aggregate-stat updates, and sample attachment are excluded.
The adapter captures the current `ActionCorrelation` once and carries that
capability across the synchronous WGPU round trip, so asynchronous device work
does not depend on ambient TLS after dispatch.
On the deterministic 1.77M-trible reconvergence probe (M4 Max, 16 Rayon
workers), each timed run kept 371 small rank batches on CPU and sent 54 batches
to Metal, reducing the controlled parallel-DAG median from 382 ms to 312 ms.
Forcing all 425 non-empty rank batches emitted by the shards to WGPU instead
took 775 ms, demonstrating that the admission boundary is part of the
algorithm rather than a backend detail.

A partially consumed ordinary residual or DAG query converted through
`into_par_iter()` is drained as one parallel leaf so its exact remaining state
cannot be restarted. Both explicit block-native entry points require a fresh
query. With one Rayon worker each has a zero split budget; with `N` workers each
permits at most `N - 1` splits. In every case the result guarantee is equality
of the distinct raw projected-row set, not iteration order.

The parallel paths clone the constraint tree and result postprocessor per
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
explores those choices depth-first; the DAG stable-partitions the same exact
per-row variable choices and files their results by bound-variable set, so
histories may reconverge only after their selected actions run. The residual
engine additionally makes the exact proposer occurrence and remaining
confirmer set part of canonical state. Because every path refreshes estimates
during evaluation, binding order adapts whenever a constraint updates its
influence set—there is no separate planning artifact to maintain.
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
