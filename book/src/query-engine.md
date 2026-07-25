# Query Engine

Queries describe the patterns you want to retrieve. The engine favors
predictable latency and skew resistance without a separately compiled query
plan. Every operator and data source implements the same
[`Constraint`](triblespace::core::query::Constraint) protocol, and the engine
consults those constraints while it searches. Binding order can therefore
adapt to the values already found instead of being fixed before evaluation.

The current protocol is **block-native**. Its unit of work is not necessarily
one partial binding, but a block of partial bindings that have the same set of
bound variables. Every live ordinary iterator uses one canonical
residual-state machine. Narrow states naturally call the same protocol with a
single row; reconverged states can call it with larger row blocks. A constraint
therefore has one implementation whether its probes are issued one at a time,
fused into a CPU loop, or dispatched to a batch-oriented accelerator.

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

When the engine asks for candidates for another variable, the tagged form of
[`CandidateSink`](triblespace::core::query::CandidateSink) stores a ragged
matrix as `(row, value)` pairs:

```text
(0, E1), (0, E2), (2, E7)
```

Here row 0 has two extensions, row 1 dies, and row 2 has one. Pairs remain
grouped by row. A one-row caller instead uses the plain-values sink, where the
row index is statically zero and no tag is stored. Estimates follow the same
pattern through [`EstimateSink`](triblespace::core::query::EstimateSink): a
multi-row action receives one estimate per row, while a one-row action can use
the compact scalar representation.

## The constraint protocol

Five operational methods and one static dependency hint perform the ordinary
query negotiation:

| Method | Responsibility |
|---|---|
| `variables` | Declare the variables the constraint touches. |
| `estimate` | Produce a candidate-count estimate for a variable and every input row. |
| `propose` | Enumerate candidate values for a variable and associate each value with its parent row. |
| `confirm` | Remove candidates that violate this constraint. |
| `satisfied` | Check the truth of a constraint whose relevant variables have become bound. |
| `influence` | Declare static estimate dependencies; their count breaks equal-magnitude variable-choice ties. |

Every `Constraint` occurrence unconditionally denotes one fixed raw-inline SET
relation over the variables it declares. `proposal_coverage` is not a second
semantic mode: it is the structural source-eligibility receipt for one
occurrence, target, and bound-variable schema. `None` makes no completeness
claim, `Covering` includes the complete existential fiber and requires
self-confirmation, and `Exact` equals that fiber. Coverage must not depend on
bound values or estimates. Every surviving non-full query state needs at least
one Covering or Exact source, while confirmation-only occurrences may remain
at `None`.

`propose_with_layout` additionally returns a `ProposalLayout` for the concrete
sink it just filled. This is only a physical uniqueness receipt: a grouped-set
layout can let the engine elide a deduplication pass, but it does not strengthen
coverage or change the denoted relation.

Five laws are load-bearing for correctness:

1. Ordinary, paged, typed-Program, and complete-equivalent routes must agree on
   the same relation. Activation-local novelty keys exposed by an accelerated
   route must be congruent for future outputs: equal keys cannot hide states
   with different relational futures.
2. `propose` is always given an **empty** sink. A composite must preserve that
   ownership when delegating. In particular, each arm of a union proposes into
   its own empty buffer before the buffers are merged.
3. `confirm` must return a subbag of its input and preserve row grouping. It
   retains every candidate occurrence belonging to the relation's existential
   fiber, and becomes exact once all occurrence variables other than the
   target are bound. It is a weak support refinement, not a candidate-bag
   homomorphism: conservative false positives may depend on the other
   candidates in the same call.
4. `satisfied` may optimistically return `true` while one of the constraint's
   variables is unbound, but `false` must prove that the row has no completion.
   It **must be exact once all variables are bound**. This includes
   zero-variable constraints, which are fully bound at the seed.
5. Every row-taking verb is a **row homomorphism**. Splitting a block into
   non-empty consecutive sub-blocks, evaluating them independently, and
   concatenating the outputs (with candidate row tags remapped) must equal
   evaluating the whole block. In particular, estimates and proposals
   concatenate, confirmation is local to each candidate's row, and whole-block
   `satisfied` is the conjunction of the sub-block answers. Batched
   implementations may fuse physical work, but block-global top-k or first-row
   decisions are invalid. Diagnostics may observe call boundaries, but those
   observations must never feed back into protocol answers.

The fourth law is easy to mistake for an optimization hook, but it is a
soundness rule. An [`or!`](triblespace::core::prelude::or) constraint uses it to
discard alternatives contradicted by the current row before those alternatives
propose or confirm another variable. An optimistic answer for a fully bound,
false alternative could otherwise admit a row that no single alternative
satisfies. A fully constant pattern similarly has no variable through which the
search could discover failure, so [`Query::new`](triblespace::core::query::Query::new)
settles it with an exact `satisfied` call against the seed block.

Estimates are cost quotes only. Returning no estimate means unknown cost, not
irrelevance, and no estimate may authorize a different proposal, confirmation,
route, or result. Structural relevance comes from `variables`; logical source
eligibility comes from `proposal_coverage`.

Constraints are otherwise stateless. Each method receives the current
`RowsView`; the engine does not notify constraints when it backtracks, chunks a
frontier, or processes work in a different order. This is what allows the same
constraint tree to run at every residual width and on either serial or parallel
executors.

## One expansion step

An expansion still performs the familiar Atreides negotiation:

1. Estimate each eligible proposal source for every unbound variable under the
   current partial bindings. Directed action costs choose the physical source
   within each variable.
2. Choose the preferred next variable with one fixed key: the selected
   source's raw candidate-count bit length (smaller first), then static
   influence count (larger first), then `VariableId` (smaller first). In a
   multi-row block this decision is made per row, because different bound
   values can imply different cardinalities.
3. Stable-partition the rows by their exact preferred variable. This preserves
   each row's selected occurrence bag while still batching rows whose preferred
   variable agrees; no row is reassigned to an estimate-similar variable.
4. For each group, propose that variable. An intersection chooses its tightest
   child per row and runs the remaining children as explicit confirmation
   actions. A union remains an opaque leaf that evaluates its still-satisfied
   alternatives independently and merges their candidates.
5. Extend the parent rows with the surviving `(row, value)` pairs. Rows without
   candidates disappear.

There is no standalone join plan or second depth-first engine. Width one is the
low-latency edge of this same execution model: one-row actions use plain-value
sinks without allocating row tags, and a surviving continuation remains hot so
the machine can descend before harvesting wider sibling cohorts.

## Canonical residual-state machine

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
explicit routes. A structurally absent route and a route deferred by policy
both use the stable ordinary `Constraint` action; neither strengthens an
ordinary proposal receipt. The formula and Program scope chains therefore form
nine structural lowering combinations.

Each canonical descriptor includes the bound-variable schema and one of four
phases:

- `Ready` jointly chooses a row's next variable and exact proposing leaf.
- `Propose` invokes one uniform proposer over an assembled parent-row bucket.
- `Candidate` chooses the next unchecked relevant confirmer.
- `Confirm` invokes one uniform confirmer over a disjoint page of the admitted
  candidate relation. A selected typed Program may retain one complete parent
  activation when doing so reuses traversal state.

Planning phases only estimate, partition, and file work; protocol calls happen
in the explicit action phases. The checked-leaf set is canonical, so histories
that applied the same constraints in different orders can append to the same
future state before its remaining work runs. Before newly proposed candidates
can split into independent pages, the engine reverse-stably admits one
`(parent row, value)` occurrence. Equal values under different affine parents
remain independent. Confirmers may conservatively retain different false
positives for different pages, but must preserve every true support and become
exact under their fully bound schema, so correctness depends only on the final
raw SET rather than intermediate payload or trace equality. Formula OR retains
its private ordered-set reducer and live-frame payload barrier; a repeated RPQ
may retain one complete parent activation solely to reuse graph-product
traversal. Segmented affine payloads cross through a bounded engine admission
phase rather than synchronous materialization. The terminal projection remains
the universal final SET guard across hidden witnesses and routes.

Lazy residual execution begins with actionable width one. A surviving action
keeps its newly filed continuation hot, allowing a successful path to descend
and emit before cold siblings are evaluated. Dead actions and terminal rows
grow the desired width geometrically; once no hot continuation can run, an
occupancy/readiness policy harvests wider batches. This gives the state machine
a low-latency-to-throughput ramp without requiring a complete intersection to
run eagerly for one binding.

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
An accepting initial product root is settled one step earlier. Typed Program
seeding records its endpoint in the same distinct accepted set used by later
pages and returns a one-shot effect receipt to the scheduler. A streaming
proposal or fully-bound Boolean Support reducer files that receipt into the
stable machine immediately, while the root's affine Program credit remains
live for non-epsilon paths. Activation-reuse confirmation and non-linear
formula proposal retain their quiescence barriers: seed acceptance is private
reducer state there, not an illegally streamed result. This is the generic
`ProgramSeedWork::accepted` law, not an RPQ branch in the scheduler. It
preserves NODES(G) gating, same-variable paths, duplicate outer parent bags,
and clone/drop remainders. Seed publication consumes no later page budget, and
the first resumed state cannot replay it. Conversely, an independently dead
Program page still supplies geometric negative feedback even if the activation
published an earlier seed effect.

Paged product states cross one block-native typed Program seam. The erased
batch carries row-aligned opaque work handles, immutable parent context, and
ragged limits whose sum is the current global width; typed effects return with
input tags. Storage and accelerator implementations may fuse a cohort without
changing canonical state, novelty, rank, or producer-credit semantics.

Final-variable streaming activations use a terminal physical policy on that
same seam. A directed hot continuation advances exactly one affine activation
with its activation-local sparse quantum `t_a`, capped by global search width
`S`. Cold global harvesting may instead cohort compatible terminal
activations under `B=min(S, sum_a t_a)`, with ragged task limits that never
spend more than one activation's `t_a` on its behalf. The backend call is
shared, but feedback is not: an activation that publishes resets to one
independently of a sibling whose live miss doubles its own quantum. A negative
terminal cohort reaches outer search-width growth only after it saturates `S`
and leaves work live.

The ordinary [`Query`](triblespace::core::query::Query) uses this engine whenever
exact seed settlement leaves a live search. Opaque roots, one-leaf ANDs,
disjoint conjunctions, finite Union roots, RPQ roots, and live zero-variable
truths therefore all exercise the same residual substrate. A seed-rejected
query starts no worklist at all. Production lowering flattens exposed
associative AND regions, lowers finite Union leaves and their recursive
AND/OR descendants into continuations, and enables production-qualified typed
Programs for RPQs and other heterogeneous actions.
Canonical single-shard SuccinctArchive Propose, Confirm, and Support routes are
production-qualified, so their pageable typed form participates in ordinary
execution. Program retirement validates a wider activation receipt with
one arena membership pass, avoiding the previous activation-count by arena-size
multiplier while retaining cheap singleton and fully drained paths.
UnionArchive Propose and Support routes are `Production`; Confirm remains
`Explicit`. Propose normally retains sparse, geometrically widened shard
paging for low-demand and nonterminal work. A fresh multi-parent terminal
cohort may instead use `CompleteActionEquivalent`, preserving the exact
parent-major then shard-major raw occurrence bag until parent-local SET
admission. Dense complete drains and bounded Succinct proposal pages consume
the same already-located Ring walk. A resident WGPU two-bound proposal is a
distinct preferred production family; a structurally declined action falls
back to the canonical production Succinct route.

Ordinary [`Query`](triblespace::core::query::Query) iteration owns the residual
cursor for every root. Its compiler policy is fixed: native AND flattening,
finite Union-leaf continuations, and production-qualified Programs.
`solve_residual_state_lazy` uses the same production plan while exposing width
controls; `solve_residual_state_lazy_with` remains an explicit compiler probe;
`solve_residual_state` is the eager saturated form, and
`solve_residual_state_profiled` reports state, merge, action, and batch
measurements. Fully drained variants preserve the distinct raw projected-row
set, but may change result order. Fully-bound rows remain raw until the
consumer pulls them, so the worklist never stores projected `R`s and a
partially consumed query can snapshot its exact remainder without requiring
`R: Clone`.

## Terminal projection and SET identity

Every semantic action admits a SET before publishing successors. Proposal
actions remove duplicate values for each affine parent, and residual sources
and transitions perform the same admission at their stable boundary. Internal
probes may still carry occurrence bags before that boundary, but every complete
raw binding is therefore unique when it reaches projection.

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

With the `parallel` feature, ordinary `IntoParallelIterator` consumption uses
the same canonical residual runtime as ordinary serial iteration. A fresh
query starts with the adaptive geometric width policy and partitions its exact
affine frontier into at most one shard per worker. Rows and SET-admitted
candidate occurrences are the same shard atoms used by the explicit residual
path. A selected typed Program may retain one complete parent activation for
physical traversal reuse, and a live Formula OR frame retains its private
payload.
Cross-shard reconvergence is traded for concurrency, but no second solver or
seed restart is involved.

[`Query::into_par_residual_state_iter`](triblespace::core::query::Query::into_par_residual_state_iter)
is the explicit saturated-width residual entry point. It uses the same affine
splitter and executor as ordinary parallel iteration, but treats the call as a
full-enumeration throughput request and starts at the width cap. Rows and
SET-admitted candidate occurrences are valid shard atoms; a selected typed
Program may keep one complete parent activation intact for physical traversal
reuse, and a live Formula OR frame retains its private payload. Every shard
retains canonical state merging locally; state is moved rather than
duplicated, and the constraint/postprocessor pair is cloned only when a real
sibling shard is created. Both ordinary and saturated parallel entry points
use the same fixed production plan as serial `Query`; compiler-policy
experiments construct a `ResidualStateIter` explicitly with
`solve_residual_state_lazy_with`.

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
The `residual_reconverge_bench` example measures this admission boundary across
adaptive and saturated serial/Rayon residual execution. It compares exact
sorted output before timing and reports the CPU, forced-WGPU, and thresholded
hybrid paths separately rather than treating executor choice as a planner
mode.

A partially consumed ordinary query converted through
`into_par_iter()` is drained as one parallel leaf so its exact remaining state
cannot be restarted or partitioned by a second solver. The explicit saturated
block-native entry point requires a fresh query. With one Rayon worker it has a
zero split budget; with `N` workers it permits at most `N - 1` splits. In every
case the result
guarantee is equality of the distinct raw projected-row set, not iteration
order.

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
providing skew-resistant and predictable performance. The residual machine
makes the exact proposer occurrence and remaining confirmer set part of
canonical state so equivalent futures can reconverge after their selected
actions run. At width one it follows the hot continuation depth-first; as width
grows it batches equivalent futures. Every Ready state computes fresh
per-row estimates for all unbound variables; the static `influence` cardinality
is only the equal-magnitude tiebreak. There is no cached estimate state or
separate planning artifact to maintain.
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
