# Regular Path Indexes

Regular path queries ask whether two graph terms are connected by a path whose
edge labels are accepted by a finite automaton. TribleSpace keeps that recursive
work outside the core query solver: the standalone `triblespace-paths` crate
materializes the accepted endpoint relation, then exposes it as an ordinary
two-variable [`Constraint`](triblespace::core::query::Constraint).

This separation keeps query-time constraints stateless. The expensive fixpoint
is built once per graph snapshot; `find!`, `and!`, constants, and the normal
dynamic variable ordering then treat the result like any other relation.

Add the companion crate alongside the facade crate:

```toml
[dependencies]
triblespace = "0.47"
triblespace-paths = "0.47"
```

## Describe the path, then materialize it

Most callers describe a regular path with `PathExpr`. Each leaf is a graph
property `Step`; the expression builders add concatenation, alternatives, and
repetition. `compile` freezes that description into the fixed, epsilon-free
automaton consumed by `PathIndex` and `PathRollup`.

For example, `friend+` means one or more forward `friend` edges:

```rust,ignore
use triblespace::prelude::*;
use triblespace::prelude::inlineencodings::GenId;
use triblespace_paths::{PathExpr, PathIndex, Step};

mod social {
    use triblespace::prelude::*;
    use triblespace::prelude::inlineencodings::{GenId, ShortString};

    attributes! {
        "A19EC1D9DD534BA9896223A457A6B9C9" as pub name: ShortString;
        "C21DE0AA5BA3446AB886C9640BA60244" as pub friend: GenId;
    }
}

let friend = social::friend.id().into();
let friend_plus = PathExpr::from(Step::Forward(friend)).plus();
let friend_automaton = friend_plus.compile();

let alice = fucid();
let bob = fucid();
let carol = fucid();
let mut graph = TribleSet::new();
graph += entity! { &alice @ social::friend: &bob };
graph += entity! { &bob @ social::friend: &carol, social::name: "Bob" };
graph += entity! { &carol @ social::name: "Carol" };

let paths = PathIndex::from_tribles(friend_automaton.clone(), graph.iter())?;
```

Every trible is viewed as a directed graph edge from its entity to its inline
value, labeled by its attribute id. Values do not have to encode entity ids,
although a path endpoint must use a compatible inline encoding when it shares a
query variable with another constraint.

The expression operations are regular-language operations:

- `a.then(b)` matches `a` followed by `b`;
- `a.or(b)` matches either expression;
- `star`, `plus`, and `optional` mean zero-or-more, one-or-more, and
  zero-or-one repetitions; and
- `inverse` reverses the complete path. It flips each atomic step, reverses
  sequence order, distributes over alternatives, and preserves repetition.

`Step::Forward` and `Step::Reverse` match one exact attribute in either graph
direction. `ForwardExcept` and `ReverseExcept` match every attribute except a
provided list; an empty exclusion list is a wildcard, available as
`Step::forward_any()` or `Step::reverse_any()`. Inverting an exclusion or
wildcard changes only its direction.

## Canonical expressions and compilation

`PathExpr` canonicalizes structure as it is assembled. Nested sequences are
flattened while retaining their order. Nested alternatives are flattened,
sorted by a stable explicit order, and deduplicated. Exclusion lists are also
sorted and deduplicated. Thus independently assembled expressions that differ
only by alternative ordering, duplicate alternatives, sequence association, or
exclusion ordering compile to the same canonical automaton and fingerprint.

This is **structural** canonicalization, not regular-language minimization.
Distributively equivalent expressions, or identities such as a nested star,
may still compile to different language-equivalent automata. Do not use
automaton equality to decide arbitrary regular-language equivalence.

Compilation uses the Glushkov position construction. State zero is the sole
initial state, and each atomic `Step` occurrence contributes one additional
state. First-position and follow-position relations become transitions;
nullable expressions make state zero accepting. The result is a fixed NFA with
no epsilon transitions and no determinization pass. Repetition changes the
finite follow relation rather than unrolling an unbounded machine.

The current high-level surface is the Rust builder API, not a string parser or
`path!` macro. Every `PathExpr` contains at least one atomic step, so a pure
epsilon language or the empty language must be expressed with a manual
automaton. State ids are `u32`; `compile` panics if the expression contains
`u32::MAX` atomic occurrences, because no valid `Automaton` can represent it.

## Manual automata are the low-level escape hatch

Construct `Automaton` directly when importing the output of another compiler,
when a deliberately shared state topology matters, or when the language has no
atomic step. The `friend+` expression above is equivalent to this explicit
two-state NFA:

```rust,ignore
use triblespace_paths::{Automaton, Step, Transition};

let friend_automaton = Automaton::new(
    2,
    [0],
    [1],
    [
        Transition::new(0, 1, Step::Forward(friend)),
        Transition::new(1, 1, Step::Forward(friend)),
    ],
)?;
```

`Automaton::new` validates state numbers and canonicalizes duplicate or
out-of-order transitions and exclusion lists. Its input must already be
epsilon-free. Represent nullability by making an initial state accepting; for
example, one initial-and-accepting state with no transitions accepts only the
empty path.

## Join paths with ordinary constraints

`PathIndex::constraint(start, end)` creates the two-column relation. Either
term may be a query variable or an inline constant, and using the same variable
twice asks for the accepted diagonal. The relation composes directly with
`pattern!` and every other constraint:

```rust,ignore
let alice_value: Inline<GenId> = (&alice).to_inline();

let reachable_people: Vec<(Id, String)> = find!(
    (person: Id, name: String),
    and!(
        paths.constraint(alice_value, person),
        pattern!(&graph, [{ ?person @ social::name: ?name }]),
    )
)
.collect();
```

The index also has direct read methods when no join is needed:
`contains`, `reachable_from`, `reaching`, `accepted_pairs`, `starts`, `ends`,
and `diagonal`. All endpoint fibers are sorted and duplicate-free. The path
relation therefore contains one pair per accepted `(start, end)`, not one row
per distinct route between them; ordinary query joins can still introduce bag
multiplicity through their other witnesses.

## Build from a resolved repository view

Branch publication and derived-index maintenance are separate operations. The
assertion-native `Repository` has no hidden on-push hook: publish the source,
resolve its exact branch identity, then build the path relation from the
complete checkout you intend to index:

```rust,ignore
use triblespace_paths::PathIndex;

let identity = repo.branch_identity("main");
let mut ws = repo.create_workspace("main")?;
ws.commit(graph, "add social graph")
    .expect("workspace rank has room");
repo.push(&mut ws)?;

let mut resolved = repo.pull(identity)?;
let facts = resolved.checkout(..)?;
let paths = PathIndex::from_tribles(friend_automaton, facts.iter())?;
```

`Repository::pull` succeeds only for a complete frontier. That makes the
checkout's source boundary explicit: a missing asserted tip or unresolved
ancestry cannot silently produce a derived index that claims to be current.
For persistent incremental indexes, `PathRollup` remains the typed range
recipe, but the application must run range construction and publication as an
explicit derived-data workflow. It is not part of the source assertion's
durable append point.

## What a persisted summary means

The automaton is part of the recipe identity. Two `PathRollup`s with different
automata have different fingerprints, manifests, and range artifacts even when
they cover the same commits.

Each nonempty range stores a canonical `PathSummaryBlob` containing only:

- the sorted endpoint domain required by the fixed automaton, and
- the sorted direct arcs of the graph × automaton product.

Those summaries are sparse constructional data, not independently closed path
relations. Compaction is canonical set union. At attachment,
`PathRollup::attach_exact` unions every live range summary and computes the
accepted endpoint relation once over the whole union. That order is essential:
one path may take its first edge from range A, its next edge from range B, and
later re-enter A. Unioning closures built independently per range would miss
such paths.

This design also makes merge order irrelevant. `PathSummary::merge` is
associative, commutative, and idempotent for one fixed automaton; closure is
derived only after the summaries have been combined.

## Nullable paths and the vertex universe

A nullable expression uses an accepting initial state. Its zero-hop answers are
the identity pairs `(v, v)` for the summary's complete vertex universe. The
universe includes both endpoints of every supplied trible, even when that
trible's attribute matches no automaton transition. Without those unmatched
terms, a nullable index would incorrectly lose valid zero-hop answers.

Non-nullable summaries omit those unmatched endpoints entirely. Nullable
summaries retain them as the identity universe, but the SCC and bitset closure
still runs only over endpoints incident to matching product arcs; the index
then maps that relation back into the full universe and adds the diagonal.
Unrelated attributes therefore do not widen the quadratic closure workspace.

An entirely empty source has no graph terms and therefore no identity pairs. Its
range still exists as a certified contentless record, but it has no
`PathSummaryBlob` handle. “Covered and empty” is distinct from “not indexed.”

## Freshness and the trust boundary

A durable consumer must bind a manifest to the exact complete source frontier
it covered, validate every summary's canonical bytes and automaton fingerprint,
and reject stale coverage. Imported or manually assembled metadata should be
checked with `Manifest::audit_exact_cover` against a blob reader before it is
trusted, or rebuilt. See [Range-Native Derived Indexes](index-ranges.md) for the
inclusive frontier and exact-cover rules. This audit belongs to the derived
index workflow; source branch assertions remain a small, index-neutral ledger.

## Cost model: sparse input, potentially dense answer

Range summaries retain an endpoint domain and direct product arcs, so they can
remain close to the sparse input and merge cheaply. Attachment is a different
operation: it materializes the complete accepted endpoint relation as CSR plus
reverse and domain views. Some regular paths accept every pair of vertices,
making that relation Θ(|V|²). No exact materialized representation can avoid
paying for that output, and the closure construction also uses bitset scratch
space.

The current canonical blob stores product endpoints as full-domain `u32`
ordinals. Persisted nullable summaries consequently require
`|universe| × |automaton states| <= u32::MAX`, even though attachment closes
only the smaller matched support. Crossing that format ceiling is an explicit
error rather than ordinal truncation.

Use a `PathRollup` when the automaton is stable and many queries will amortize
attachment, or when fast membership and joins matter. For a
one-off traversal on a large sparse graph, an application-side graph search may
use less memory; for a fixed small number of hops, explicit `pattern!` clauses
remain the simplest answer. The path index is a deliberate materialized-view
trade, not a hidden lazy traversal.
