# Query Engine

Queries describe the patterns you want to retrieve. There is no query planner
and no compiled plan. Every operator and every data source implements the same
[`Constraint`](triblespace::core::query::Constraint) protocol, and the engine
consults those constraints *while* it searches, so binding order is chosen from
the values already found instead of being fixed before evaluation.

This chapter describes that protocol, the search that drives it, the result
semantics that fall out of both, and — just as importantly — the things the
engine deliberately refuses to do. The refusals are load-bearing: each one is
what buys a property the engine does provide.

## The constraint protocol

A constraint restricts the values that query variables may take. It is not a
node in a plan; it is a participant the engine interrogates. The whole
interface is seven methods, and the engine calls them in a fixed rhythm:

| Method | Role | Called |
|---|---|---|
| `variables` | Declares which variables the constraint touches. | Once, at query start. |
| `estimate` | Predicts the candidate count for one variable under the current binding. | Before each binding decision. |
| `propose` | Enumerates candidate values for a variable into a buffer. | On the tightest constraint for that variable — an intersection picks it among its children. |
| `propose_chunk` | Resumable `propose`: appends up to a budget of further candidates. | Instead of `propose`, when the engine wants the level in pieces. |
| `confirm` | Kills candidates that violate this constraint. | On every *other* constraint touching that variable. |
| `satisfied` | Reports whether the constraint is still consistent with the binding. | Once at query start on the whole tree; then by a union before it proposes or confirms, to skip dead arms. |
| `influence` | Names the variables whose estimates go stale when one variable is bound. | Once per variable, at query start. |

`estimate` returns `None` for a variable the constraint does not touch, which
is how "irrelevant" is distinguished from "unknown cost". An estimate is a cost
quote and nothing else: it steers variable ordering, never correctness. A
constraint that lies about its cardinality makes the search slower, not wrong.

Only four of the seven need an implementation. `propose_chunk` defaults to
"deliver everything on the first call and report exhaustion", `satisfied`
defaults to `true`, and `influence` defaults to "every variable I touch except
this one". A new data source therefore joins the engine with `variables`,
`estimate`, `propose`, and `confirm`.

Composition is by two constraints rather than by an algebra:
[`IntersectionConstraint`](triblespace::core::query::intersectionconstraint::IntersectionConstraint)
(built by [`and!`](triblespace::core::prelude::and)) and
[`UnionConstraint`](triblespace::core::query::unionconstraint::UnionConstraint)
(built by [`or!`](triblespace::core::prelude::or)). Both are ordinary
constraints implementing the same seven methods, so a `TribleSet` pattern, a
`HashSet` membership test, and an application predicate mix in one query
without any of them knowing about the others.

## Statelessness is the load-bearing choice

Every method receives the current [`Binding`](triblespace::core::query::Binding)
as a parameter. A constraint keeps no cursor, no half-finished enumeration, no
record of where the search has been.

That single decision pays for most of the engine's structure:

- **Backtracking is free.** The engine unsets a variable in the binding and
  moves on. There is nothing to notify, unwind, or roll back, because no
  constraint holds state that could be stale.
- **The constraint tree can be cloned.** Parallel execution splits by cloning
  the *engine's* state; the constraint tree is shared behind an `Arc`. If
  constraints held live iterators, a split would have to duplicate or hand off
  those iterators, and a borrowed enumeration would tie the constraint's
  lifetime to the engine's — the self-referential trap that a stateful protocol
  cannot avoid.
- **Resumption is plain data.** When the engine wants a level in pieces it
  hands `propose_chunk` a
  [`ProposeCursor`](triblespace::core::query::ProposeCursor): a `started` flag
  and 32 opaque bytes the source interprets however it likes (a last-delivered
  value, a rank offset). The source re-finds its place from that cursor on
  every call. Because the cursor is POD, it survives `Query::clone` and the
  rayon splitter without any cooperation from the source.

The price is that a source with an expensive seek pays it again on every
resume. That is the trade the engine takes, and the geometric chunk schedule
below is what keeps the number of resumes logarithmic.

## Depth-first search with dynamic variable ordering

The ordering is the engine's core performance idea, not an implementation
detail.

[`Query::new`](triblespace::core::query::Query::new) asks every variable for an
estimate against the empty binding and sorts the unbound set by it. Then the
search repeats one step:

1. Refresh the estimates that the most recent binding could have changed — the
   `influence` sets of the variables bound since the last refresh, minus the
   ones already bound.
2. Re-sort the unbound variables and take the most specific one.
3. Ask the constraint tree to `propose` candidates for it. An intersection
   internally lets its tightest child propose and runs the remaining children
   as confirmers over that child's output, so the buffer the engine sees has
   already survived every clause.
4. Bind the first live candidate and descend.
5. When a level runs out of live candidates and its source is exhausted, unset
   the variable, push it back into the unbound set, and continue with the next
   candidate one level up.

Specificity is deliberately coarse. The sort key is the *bit length* of the
estimate (`ilog2(n) + 1`), so counts inside the same power-of-two bucket are
treated as equally specific; the tie-break then prefers the variable with the
largest `influence` set — the one whose binding will sharpen the most other
estimates. Two effects follow. Small differences between two sources' guesses
cannot flip the order, which keeps the search stable when estimates are rough.
And when the engine genuinely cannot tell two variables apart on cardinality,
it picks the one that buys the most information.

Re-sorting on every step is what makes the ordering *dynamic*. A planner
chooses one order from global statistics and lives with it for the whole query;
here each level chooses from the estimates *under the current partial binding*.
On skewed data this is the difference between a good average and a good worst
case: the popular entity and the rare one take different paths through the same
query, because after binding an entity the remaining estimates are no longer
the same numbers. Nothing is cached, so nothing has to be invalidated. See the
[Atreides Join](atreides-join.md) chapter for how estimate fidelity ranks and
why this is worst-case optimal.

## Proposals: one write-once buffer per level

`propose` writes into a
[`ProposalBuffer`](triblespace::core::query::ProposalBuffer) — the engine's
candidate store for one variable at one level. Entries are plain 32-byte
`RawInline` values at fixed stride. Alongside them the buffer keeps a liveness
bit per entry, packed 32 to a `u32`.

Entries are effectively write-once. A proposer may rewrite the region it
appended in the current call before it returns — that is how a union applies
its sort-dedup — but once the caller can see the region, the indices are
frozen, because kills bind to them.

Nothing is ever compacted. A candidate that fails confirmation has its liveness
bit cleared and stays exactly where it was; the engine iterates the live
entries. That costs a scan over dead entries and buys two things: an entry's
index is stable for the lifetime of the level, so a kill can be recorded by
index alone, and no confirmer ever has to agree with another about where a
candidate now lives.

Packing liveness 32 to a word is what makes that scan cheap: `count_live` and
`next_live` fold whole words through `count_ones` and `trailing_zeros` rather
than looking at candidates one at a time. Two things get harder in exchange,
and both are paid for inside the buffer's own module. A kill becomes a
read-modify-write on a word shared with 31 neighbours. And a region no longer
starts on a word boundary, so its first and last words carry liveness bits
owned by *neighbouring* regions of the same buffer. The invariant is enforced
at the boundary of the type: every write path masks to the owned bits and every
read path zeroes the bits it does not own, so no caller — the device confirm
path included, see below — can reach a neighbour's liveness.

## Confirmation is kill-only

[`Candidates`](triblespace::core::query::Candidates) is the region handed to
`confirm`: values are read-only, liveness words are killable, and there is no
way to revive an entry or add one.

That restriction is the whole reason confirmation needs no coordination. If a
confirmer can only remove, then several confirmers writing into the same region
compute their conjunction no matter how they are scheduled:

- **Sequentially**, each skipping entries that are already dead — the CPU path.
- **In parallel**, each on its own copy of the liveness words, merged with
  [`and_words`](triblespace::core::query::and_words) — the path a batching
  accelerator takes.

Both schedules are legal, produce identical liveness, and the engine does not
have to choose between them ahead of time. A union inverts the merge: it runs
each live arm on a scratch copy and combines with
[`or_words`](triblespace::core::query::or_words), so a value survives if any
arm accepts it.

This is also what makes an accelerator safe to bolt on. A device that computes
verdicts for a whole region cannot corrupt the search, because the only thing
it can do with its answer is clear bits that the CPU would also have cleared.

## Chunked proposing

A level whose source has a million candidates should not have to materialize a
million candidates before the engine can try the first one. `propose_chunk`
lets the engine pull a level in pieces: the first request asks for 64
candidates, and each refill asks for four times the previous budget.

Geometric growth keeps both ends bounded. Time-to-first-result at a wide level
is bounded by the first chunk rather than by the level's cardinality, while the
total enumeration work stays within a constant factor of eager proposing and
the number of refill calls stays logarithmic in the level width.

The contract on the source is small but strict: never deliver the same value
twice across the calls of one enumeration (that would inflate row multiplicity,
see below), and always advance the cursor when the budget is nonzero, so a
caller that loops on refill terminates.

## Parallel execution

With the `parallel` feature a query is also a rayon producer:
`find!(...).into_par_iter()`. There is no second solver behind it. Splitting
walks the same state machine and hands one half of a level's candidates to a
sibling:

- While the top of the search stack has a single pending candidate *and* its
  source is exhausted, bind it and descend. Descending through a level whose
  cursor is still live would clone that cursor into the sibling and enumerate
  its tail twice, so such a level is bisected instead.
- When the top has two or more pending candidates, bisect them. The right half
  takes the materialized tail and is marked exhausted; the left half keeps the
  cursor. Every unmaterialized candidate therefore has exactly one owner.
- A leaf just drives the ordinary sequential `Iterator::next` and folds the
  results. No engine logic is duplicated for the parallel path.

Because rayon resets its splitter budget on every stolen task, each producer
carries its own bounded budget — `num_threads²`, halved at each split — so a
busy pool cannot drive the split tree arbitrarily deep against a query that
always has more candidates to bisect.

The guarantee is the same bag of rows, not the same order. Constraint trees are
shared behind an `Arc`, so a split is a refcount bump rather than a tree clone;
code that wants aggregate observations across shards needs its own
synchronization (an `Arc<AtomicU64>`, say) because clone-local interior state
is not a global counter.

## Bag semantics at the interface

**The engine emits one row per complete binding.** When the unbound set empties,
that assignment is a result. Nothing deduplicates it.

Hidden variables therefore surface as multiplicity. If an entity has *n*
outgoing edges and a query projects only the entity while a `temp!` or `_?`
variable ranges over the target, that entity is emitted *n* times:

```rust
use std::collections::HashSet;

use triblespace::prelude::*;

mod social {
    use triblespace::prelude::*;
    attributes! {
        "C21DE0AA5BA3446AB886C9640BA60244" as friend: inlineencodings::GenId;
    }
}

let mut kb = TribleSet::new();
let alice = ufoid();
let bob = ufoid();
let carol = ufoid();
kb += entity! { &alice @ social::friend: &bob };
kb += entity! { &alice @ social::friend: &carol };

// One row per complete binding: the hidden `_?friend` multiplies `?person`.
let rows: Vec<_> = find!(
    (person: Id),
    pattern!(&kb, [{ ?person @ social::friend: _?friend }])
)
.collect();
assert_eq!(rows.len(), 2);

// Deduplication is the consumer's job.
let distinct: HashSet<_> = find!(
    (person: Id),
    pattern!(&kb, [{ ?person @ social::friend: _?friend }])
)
.collect();
assert_eq!(distinct.len(), 1);
```

This replaced an engine that projected with SET semantics. That engine kept a
claims table: an ordered key derived from the head's raw bytes, claimed before
conversion, so a second binding with the same public identity was discarded.
It was removed because the cost was structural rather than incidental. The
table's memory grows with the *result* set, not with the query; under rayon the
claim domain has to be shared across workers, which puts a synchronization
point on the hot path of an otherwise share-nothing search; and once user code
runs behind a claim, a conversion failure or a panic consumes the key, so
another witness cannot retry it — a rule that is difficult to explain and
easy to trip over. Worst of all, the multiplicity is genuine information about
the data, and the engine was throwing it away on the way out.

Bag semantics is not the absence of a feature so much as the decision about
where the feature belongs. Two idioms cover what the claims table used to do:

- **Collect into a set.** `HashSet<_>` (or `BTreeSet<_>`) after `find!` costs
  memory proportional to the distinct results — the same memory the claims
  table cost — but only when the caller actually wants it, and it deduplicates
  on the converted Rust values rather than on raw bytes.
- **Two queries.** Enumerate the outer variable, and use
  [`exists!`](triblespace::core::prelude::exists) for the inner condition.
  `exists!` stops at the first witness, so the fan-out is never enumerated at
  all. This is usually the faster answer, and it is the one that reads like the
  question being asked: "entities that have a friend", not "entities paired
  with a friend, deduplicated".

The unit head follows the same rule rather than getting an exception:
`find!((), constraint)` yields one `()` per satisfying assignment. Use
`exists!` when the question is existence.

Note that `or!` is a genuine exception at the *binding step*, not at the
interface: a union sorts and deduplicates the candidate values it proposes for
a single variable, so two arms that offer the same value for the same variable
contribute one candidate, not two. That is a property of the candidate buffer
for one level, and it does not extend to complete rows — the same alias
witnessed by two different entities is still two rows.

## Constants live below the variable layer

A pattern position is a [`Term`](triblespace::core::query::Term): either a
`Var` the engine solves for, or a `Const` pinned at construction. The macro
layer folds attribute constants, literal values, and constant entity ids into
`Const` terms.

Constants never enter a `Binding`, are never proposed, and are excluded from
`variables()`. They behave exactly like a variable that was already bound —
`RawTerm::position_value` returns the pinned value where it would return the
binding's value — so every backend's bound/unbound dispatch handles them with
no extra match arms.

Keeping them below the variable layer is what makes `or!` usable. Every arm of
a union must declare the same variable set, because the result schema is flat:
one row binds one variable set exactly once, so a variable that only exists in
some alternatives has nowhere to live. If a literal allocated a hidden
variable, then two arms matching different attributes would declare different
sets and the union would be rejected at construction — which was exactly the
symptom before constants became Term-native. As folded constants they cost no
variable, so an arm on `profile::nickname` and an arm on `profile::display_name`
declare the identical set and compose.

Two more consequences fall out. Literals do not consume the 128-variable
budget, so a pattern with 161 constants allocates zero variables. And a pattern
whose positions are *all* constant has an empty variable set, which the search
would never visit — so `Query::new` settles it once, up front, with a single
`satisfied` call against the empty binding. This is why `satisfied` must be
exact once every variable it touches is bound: for a constant subtree there is
no variable through which the search could later discover failure.

## Where the GPU fits

The optional `triblespace-gpu` crate accelerates one operation:
`WgpuSuccinctArchive` keeps a succinct archive's value universe, per-axis
occupancy boundaries, and six Jerky wavelet matrices resident on the device,
and routes a `confirm` region to a kernel when it has at least
`DEFAULT_MIN_CONFIRM_BATCH` live candidates.

It is not a second engine and not a planner. Estimates, proposals, prefix
walks, and satisfaction checks stay on the CPU; a region below the threshold
and any device error fall through to the canonical CPU arm, which the crate's
parity suite holds to identical liveness words. The substitution is legal
precisely because of the kill-only contract: verdicts computed anywhere merge
back by word-wise AND, and a device can never revive a dead entry.

The packed liveness layout shapes how a kernel writes its verdicts. The flat
index is the *bit position* in the region's liveness words — so candidate `i`
sits at bit `bit_offset + i` — and one `plane_ballot` per 32-lane plane
produces a whole packed verdict word with every bit already in place, stored by
a single lane. No rotation, no read-modify-write, no atomic. The AND and the
write-back are ordinary word operations, because the device works on a
*private* copy taken through `live_words()` and merged through
`set_live_words()`, and those two mask the neighbouring regions' bits out and
back in. The kernel needs one device property for this — planes exactly 32
lanes wide — which the host checks before dispatching, demoting to the CPU arm
if it does not hold.

The threshold is measured, not guessed. On an Apple M4 Max (Metal via wgpu),
against a 262,135-trible archive with fully live regions, the GPU round trip is
nearly flat at ~1.4–2.2 ms while CPU cost scales linearly — putting the
crossover near 8k live candidates for the range shape (two wavelet ranks per
candidate) and near 22k for the lighter membership shape. 16,384 is the
single-knob compromise between them; the full crossover table lives in the
constant's doc comment. `WgpuSuccinctArchive::stats` exposes dispatch and
fallback counters, so the routing economics stay observable instead of being
hidden inside a heuristic.

## Where regular paths went

Regular path queries — "everyone reachable through a chain of `follows`", "all
ancestors via repeated `parent`" — are no longer part of the engine. The
`path!` macro and its query-time evaluator have been removed.

The reason is the stateless protocol. A triple pattern is a relation over a
fixed set of variables, and the engine can ask it for candidates under any
binding. A regular path is an automaton product traversal: evaluating it inside
the search needs per-activation state — where the frontier is, which automaton
branch produced which value, what has already been visited — and that state
belongs to one live traversal, not to a binding the engine can hand back later.
Every attempt to keep it inside the protocol grew the protocol, and the growth
did not stay confined to paths: pager hooks, activation receipts, and novelty
keys became things *every* constraint had to reason about, including the ones
that only ever wanted to answer "is this triple present".

So paths moved out of query time entirely. The replacement is a materialized
closure **index**: compile the graph edges and an epsilon-free automaton into a
product graph and maintain its reflexive transitive closure, then let queries
read the closure as an ordinary relation. Reachability becomes a lookup rather
than a traversal, which is the right shape for a data model where facts are
only ever added — the closure grows monotonically with the edge set.

The stable implementation lives in the standalone `triblespace-paths` crate.
Its `PathExpr` builder compiles the normal regular-language operations into a
fixed epsilon-free automaton; direct `Automaton` construction remains the
low-level escape hatch. The crate persists unionable direct-product summaries
per repository range, closes their global union, and exposes the resulting
endpoint relation through the ordinary two-variable constraint protocol. See
[Regular Path Indexes](regular-path-indexes.md) for the expression API,
lifecycle, freshness boundary, and dense-output trade-off.

## What the engine will not do

Four refusals, and what each one buys:

- **No cost-based optimizer.** There is no plan to compile, no statistics to
  collect, no cardinality model to keep calibrated, and no plan cache to
  invalidate. What replaces it is the per-step ordering above, which sees the
  actual partial binding instead of a summary of the data. The cost is that a
  genuinely bad `estimate` cannot be corrected by anything but a better
  `estimate`; the benefit is that adding a data source means implementing four
  methods, not teaching a planner about a new operator.
- **No negation.** There is no `MINUS`, no `FILTER NOT EXISTS`, no `OPTIONAL`.
  This is a data-model decision reaching up into the engine: the trible model
  is monotonic, and a non-monotonic operator would make a query's answer
  depend on facts *not* being present — which stops being a stable statement
  the moment another replica merges in. Monotonicity is what makes
  `pattern_changes!` sound, what makes distributed merge coordination-free, and
  what makes a query result something you can still believe after a `pull`.
- **No query-time recursion.** See the section above. Recursion returns as a
  maintained index, where the fixpoint is computed once against the data rather
  than repeatedly inside every search.
- **No projection dedup.** Covered above: the multiplicity is real, the
  deduplication has a cost, and the consumer is the one who knows whether it
  wants to pay it.

What the engine does provide in exchange is a short list, but a durable one:
predictable latency, skew resistance, no tuning knobs, and one protocol that a
`TribleSet`, a compressed on-disk archive, a `HashSet`, a search index, and an
application predicate all speak equally well.

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
providing skew-resistant and predictable performance. Estimates are recomputed
from the current binding rather than cached, so there is no invalidation
protocol and no separate planning artifact to maintain.
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
