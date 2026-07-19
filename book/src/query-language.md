# Query Language

This chapter introduces the core query facilities provided by `triblespace`.  A
query is described in a small declarative language that states which values
should match instead of spelling out the iteration strategy.  When you read a
query, you are effectively looking at a logical statement about the data: *if*
the constraints can all be satisfied, *then* the variable bindings are produced
as results.  The declarative style gives the engine freedom to reorder work and
choose efficient execution strategies.

Every macro shown here is a convenience wrapper around a concrete
[`Constraint`](triblespace::core::query::Constraint) implementation.  When you need finer
control—or want to assemble constraints manually outside the provided
macros—reach for the corresponding builder types in
[`triblespace::core::query`](triblespace::core::query).

## Declaring a query

The [`find!`](triblespace::core::prelude::find) macro builds a
[`Query`](triblespace::core::query::Query) by declaring variables and a constraint
expression. The macro mirrors Datalog syntax: the head `((...))` lists the
variables you want back, and the body describes the conditions they must meet.
A minimal invocation looks like this:

```rust,ignore
let results = find!((a), a.is(1.into())).collect::<Vec<_>>();
```

`find!` returns an [`Iterator`](core::iter::Iterator) over the bound
variables. Matches can be consumed lazily or collected into common
collections.

The head is an ordered relational projection with **SET semantics**. The
engine emits each distinct tuple of raw inline head values at most once,
regardless of how many assignments of hidden variables prove it. Distinctness
uses the raw inline bytes before Rust conversion, so different raw values that
convert to equal Rust values remain different projected rows. Conversely, a
conversion failure claims that raw tuple before filtering it: another hidden
witness for the same tuple does not retry the conversion.

When the head declares a **single variable**, omit the parentheses to get bare
values instead of 1-tuples:

```rust,ignore
for a in find!(a, a.is(1.into())) {
    println!("match: {a}");
}
```

When the head declares **multiple variables**, wrap them in parentheses to get
tuples:

```rust,ignore
for (a, b) in find!((a, b), and!(a.is(1.into()), b.is(2.into()))) {
    println!("{a}, {b}");
}
```

Adding more variables is as simple as expanding the list:
`find!((a, b, c), ...)` yields `(a, b, c)` tuples.
Variables declared in the head can be reused multiple times inside the
constraint to express joins. When a variable appears in several clauses the
engine ensures every occurrence binds to the same value. Repeating a variable in
two patterns, for example, restricts the result set to entities that satisfy
both attribute assignments simultaneously. The order of declarations defines the
shape of the tuple in the iterator, so reorganising the head changes how you
destructure results.

### Typed variables

Variables optionally include a concrete type to convert the underlying value.
The constraint phase still works with untyped [`Inline`](triblespace::core::inline::Inline)
instances; conversion happens when results are emitted.  These conversions use
[`TryFromInline`](triblespace::core::inline::TryFromInline).

By default, if a conversion fails the entire row is silently skipped — like a
constraint that doesn't match.  For types whose `TryFromInline::Error` is
[`Infallible`](core::convert::Infallible) the error branch is dead code and no
rows can ever be accidentally filtered.

Append `?` to a variable to receive the raw
[`Result<T, E>`](core::result::Result) instead. Both `Ok` and `Err` values pass
through without filtering, matching Rust's `?` semantics of "bubble the error
to the caller."

```rust,ignore
// `x` is filtered (rows where conversion fails are skipped).
// `y` is passed through as Result (no filtering).
find!((x: i32, y: Inline<ShortString>?),
      and!(x.is(1.into()), y.is("foo".to_inline())))
```

| Syntax | Meaning |
|--------|---------|
| `name` | inferred type, filter on conversion failure |
| `name: Type` | explicit type, filter on conversion failure |
| `name?` | inferred type, yield `Result<T, E>` (no filter) |
| `name: Type?` | explicit type, yield `Result<T, E>` (no filter) |

The query engine explores assignments that satisfy the constraint and yields
each distinct tuple of the declared variables in head order. Variables omitted
from the head are existential witnesses: they affect whether a tuple exists,
not how many times it is returned. A repeated variable in the head is rejected
because it would not add a new projected column.

The empty head `find!((), constraint)` therefore yields at most one `()`—one
when any assignment satisfies the body, and none otherwise. It is an existence
projection rather than a way to count satisfying assignments. Once its single
raw key is claimed, iteration stops without draining additional hidden
witnesses; this remains true when conversion or mapper code rejects or panics
on that key.

### Collecting results

Any type that implements [`FromIterator`](core::iter::FromIterator) can collect
the results of a query.  `Vec<_>` is common for tests and examples, while
`HashSet<_>` is useful when the match order is irrelevant.  When you only need
the first result, call iterator adapters such as `next`, `find`, or `try_fold`
to avoid materializing the full result set.

## Built-in constraints

`find!` queries combine a small set of constraint operators to form a
declarative language for matching tribles.  Each operator implements
[`Constraint`](triblespace::core::query::Constraint) and can therefore be mixed and nested
freely.

| Macro | Purpose | Notes |
| ----- | ------- | ----- |
| [`and!`](triblespace::core::prelude::and) | Require every sub-constraint to hold | Builds an [`IntersectionConstraint`](triblespace::core::query::intersectionconstraint::IntersectionConstraint). |
| [`or!`](triblespace::core::prelude::or) | Accept any satisfied alternative | Produces a [`UnionConstraint`](triblespace::core::query::unionconstraint::UnionConstraint) whose branches must reference the same variables. |
| [`temp!`](triblespace::core::temp) | Mint hidden helper variables | Allocates fresh bindings for the nested expression so the helpers can join across patterns without being projected. |
| [`pattern!`](triblespace::core::macros::pattern) | Match attribute assignments in a collection | Expands to a [`TriblePattern`](triblespace::core::query::TriblePattern)-backed constraint that relates attributes and values for the same entity. |
| [`pattern_changes!`](triblespace::core::macros::pattern_changes) | Track attribute updates incrementally | Builds a [`TriblePattern`](triblespace::core::query::TriblePattern) constraint that yields newly added triples from a change set because incremental evaluation stays monotonic; see [Incremental Queries](incremental-queries.md) for the broader evaluation workflow. |
| `.is(...)` | Pin a variable to a constant | Wraps a [`ConstantConstraint`](triblespace::core::query::constantconstraint::ConstantConstraint) that compares the binding against a literal value. |
| `has` | Check membership in a collection | Collections such as [`HashSet`](std::collections::HashSet) expose `.has(...)` when they implement [`ContainsConstraint`](triblespace::core::query::hashsetconstraint::ContainsConstraint); triple stores like [`TribleSet`](triblespace::core::trible::TribleSet) instead participate through [`pattern!`](triblespace::core::macros::pattern). |
| [`EqualityConstraint`](triblespace::core::query::equalityconstraint::EqualityConstraint) | Require two variables to bind the same value | Auto-desugared by `pattern!` for self-referencing patterns like `{ _?e @ link: _?e }`. |
| [`SortedSlice`](triblespace::core::query::sortedsliceconstraint::SortedSlice) | Check membership via binary search | A binary-search alternative to `HashSet` for sorted data; implements `ContainsConstraint`. |
| [`value_range`](triblespace::core::query::rangeconstraint::value_range) | Restrict a variable to a byte-lexicographic range | Builds a [`InlineRange`](triblespace::core::query::rangeconstraint::InlineRange) constraint between a min and max bound. |

Any data structure that can iterate its contents, test membership, and report
its size can implement `ContainsConstraint`. Membership constraints are
particularly handy for single-column collections such as sets or map key views,
while multi-position sources like `TribleSet` rely on `pattern!` to keep entity,
attribute, and value bindings aligned.

### Constant matches (`is`)

Call [`Variable::is`](triblespace::core::query::Variable::is) when you need a binding to
equal a specific value.  The method returns a
[`ConstantConstraint`](triblespace::core::query::constantconstraint::ConstantConstraint)
that checks whether the solver can assign the variable to the provided
[`Inline`](triblespace::core::inline::Inline).  Constant constraints behave like any other
clause: combine them with `and!` to narrow a variable after other constraints
have proposed candidates, or place them inside `or!` branches to accept
multiple literals.

```rust,ignore
find!((title: Inline<_>),
      and!(dataset.has(title), title.is("Dune".to_inline())));
```

The snippet above keeps only the rows where `title` equals `"Dune"`.  Because
`is` constrains the variable's value rather than projecting a new binding, it
is also handy for helpers such as `temp!` when you want to filter hidden
bindings without exposing them in the result tuple.

`pattern!` and `pattern_changes!` fold literal values (and attribute
constants) directly into the pattern constraint as constant
[`Term`](triblespace::core::query::Term)s — no variable is allocated for
them — so you often get the same behaviour simply by writing the desired
value in the pattern:

```rust,ignore
find!((friend: Inline<_>),
      pattern!(&dataset,
               [{ _?person @ social::friend: ?friend,
                  social::city: "Caladan" }]));
```

Repeating `.is(...)` on the same variable with different values causes the
query to fail—just as conflicting `pattern!` clauses would—so prefer `or!` (or
switch to a membership helper such as `.has(...)`) when you want to accept
several constants.

### Intersections (`and!`)

[`and!`](triblespace::core::prelude::and) combines multiple constraints that must all hold
simultaneously.  Each sub-clause can introduce new bindings or further narrow
existing ones, and the solver is free to reorder the work to reduce the search
space.  When a sub-constraint fails to produce a candidate that is compatible
with the current bindings, the whole conjunction rejects that branch and moves
on.  The macro accepts any number of arguments, so `and!(...)` is often a
convenient way to keep related clauses together without nesting additional
`find!` calls:

```rust,ignore
let favourites = favourite_titles(); // e.g. a HashSet<Id> built elsewhere
find!((book: Inline<_>, author: Inline<_>),
      and!(favourites.has(book),
           pattern!(&dataset,
                    [{ ?book @ literature::title: "Dune",
                       literature::author: ?author }])));
```

Here the membership test over `favourites` and the attribute pattern from
`dataset` run as part of the same conjunction.  The solver joins them on their
shared bindings (`book` and `author`) so only tuples that satisfy every clause
make it into the result set.  Because `and!` simply returns a constraint, you
can nest it inside other combinators such as `temp!` or `or!` to structure
queries however you like.

### Alternatives (`or!`)

Use [`or!`](triblespace::core::prelude::or) to express alternatives. Each branch behaves
like an independent constraint and may introduce additional bindings that
participate in the surrounding query, provided every branch mentions the same
set of variables:

```rust,ignore
find!((alias: Inline<_>),
      temp!((entity),
            or!(pattern!(&dataset,
                         [{ ?entity @ profile::nickname: ?alias }]),
                pattern!(&dataset,
                         [{ ?entity @ profile::display_name: ?alias }]))));
```

Each branch contributes every match it can produce given the current bindings.
The example projects each distinct alias once. A nickname and display name with
different raw values contribute two aliases; equal values collapse, as do the
same alias values witnessed by different hidden entities. Branches that cannot
match simply contribute nothing.

All branches of an `or!` must bind exactly the same set of variables;
branch-local variables are not supported. This is a consequence of the
engine's flat result schema — every result row binds the same variable set
exactly once, so there is no way to represent a variable that only exists
in some alternatives. (It is *not* a semantic or monotonicity limitation:
the union itself is monotonic.) Attribute constants and literal values do
not count towards a branch's variable set: `pattern!` folds them into the
constraint as constant [`Term`](triblespace::core::query::Term)s rather
than allocating hidden variables, which is what lets the branches above use
*different* attributes (`nickname` vs. `display_name`) while still
declaring the identical set `{entity, alias}`. Only genuine query variables
must align: if two branches reference different variables the constraint
panics at construction time, naming the mismatched sets. Note that an
anonymous entity (`{ attr: ?v }` without an `?entity @` id) introduces a
fresh variable scoped to its own `pattern!`, so bind entities explicitly —
as the example does with `temp!((entity), ...)` — when combining such
patterns with `or!`.

### Temporary variables (temp!)

Real queries often need helper bindings that participate in the joins but do
not show up in the result tuple. Wrap the relevant constraint with
`temp!((...vars...), expr)` to mint hidden variables and evaluate `expr` with
them in scope:

```rust,ignore
find!((person: Inline<_>),
      temp!((friend),
            and!(pattern!(&dataset,
                          [{ _?p @ social::person: ?person, social::friend: ?friend }]),
                 pattern!(&dataset,
                          [{ ?friend @ social::city: "Caladan" }]))));
```

The helper binding `friend` links the two patterns, ensuring the same entity is
used across both clauses without expanding the result tuple. `temp!` can create
multiple variables at once (`temp!((street, city), ...)`). You always wrap the
hidden bindings in a tuple, so each invocation reads
`temp!((...vars...), ...)`. Here `social` would be a namespace module exporting
the `person`, `friend`, and `city` attributes. The variables adopt the value
schemas implied by the constraints they appear in, so no extra annotations are
required. When working outside the query macros, call
[`VariableContext::next_variable`](triblespace::core::query::VariableContext::next_variable)
directly instead.

Because temporary variables are not part of the projection head, several
friends that prove the same projected `person` still produce that person once.
Project the witness explicitly when its identity belongs in the result.

When the helper variable lives entirely within a single pattern, consider using
`_?alias` instead of `temp!`. Both [`pattern!`](triblespace::core::macros::pattern) and
[`pattern_changes!`](triblespace::core::macros::pattern_changes) support `_?ident` placeholders that
mint fresh bindings scoped to that one macro invocation. They behave like
non-projected variables: you can reuse the `_?ident` multiple times inside the
pattern to relate attributes, but the binding vanishes once control leaves the
macro. Reach for `temp!` when the helper must span several constraints or when
you need to reuse the same hidden binding across multiple patterns.

## Example

```rust,ignore
use triblespace::prelude::*;
use triblespace::core::examples::{self, literature};

let dataset = examples::dataset();

for (title,) in find!((title: Inline<_>),
                     and!(dataset.has(title), title.is("Dune".to_inline()))) {
    println!("Found {}", title.from_inline::<&str>());
}
```

This query searches the example dataset for the book titled "Dune".  The
variables and constraint can be adapted to express more complex joins and
filters.  For instance, you can introduce additional variables to retrieve both
the title and the author while sharing the same dataset predicate:

```rust,ignore
for (title, author) in find!((title: Inline<_>, author: Inline<_>),
                             and!(title.is("Dune".to_inline()),
                                  pattern!(&dataset,
                                           [{ _?book @ literature::title: ?title,
                                              literature::author: ?author }]))) {
    println!("{title} was written by {}", author.from_inline::<&str>());
}
```

The extra variables participate in the join automatically; no explicit loop
nesting or indexing is required.

## Attribute patterns (pattern!)

The `pattern!` macro provides a concise way to match entities by attribute
assignments. It expands to a constraint that can be used directly inside
`find!`.

Important: in `pattern!` values prefixed with `?` refer to variables declared
in the surrounding `find!` head while string/number literals and more complex
expressions are treated as literal values. Use `_?name` when you need a fresh
variable that is scoped to a single macro invocation; you can reference it
multiple times within the same pattern without adding it to the `find!` head.
Parenthesised expressions remain supported for explicit literals.

```rust
# use triblespace::prelude::*;
# mod literature {
#     use triblespace::prelude::*;
#     attributes! {
#         "0DBB530B37B966D137C50B943700EDB2" as firstname: inlineencodings::ShortString;
#         "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: inlineencodings::ShortString;
#     }
# }
let mut kb = TribleSet::new();
let e = ufoid();
kb += entity! { &e @ literature::firstname: "William", literature::lastname: "Shakespeare" };

let results: Vec<_> = find!((ee: Id), pattern!(&kb, [{ ?ee @ literature::firstname: "William" }])).collect();
assert_eq!(results.len(), 1);
```

Patterns may contain multiple clauses and reuse `_?` bindings to relate
attributes without introducing extra columns in the result set.  A single
`_?person` variable can connect several attribute/value pairs while staying
scoped to the pattern:

```rust
# use triblespace::prelude::*;
# mod literature {
#     use triblespace::prelude::*;
#     attributes! {
#         "0DBB530B37B966D137C50B943700EDB2" as firstname: inlineencodings::ShortString;
#         "6BAA463FD4EAF45F6A103DB9433E4545" as lastname: inlineencodings::ShortString;
#     }
# }
let mut kb = TribleSet::new();
let e = ufoid();
kb += entity! { &e @ literature::firstname: "Frank", literature::lastname: "Herbert" };

let author_last_names: Vec<_> = find!((last: Inline<_>),
    pattern!(&kb, [{ _?person @ literature::firstname: "Frank", literature::lastname: ?last }])
).collect();
```

Here `_?person` remains scoped to the pattern while ensuring both attributes are
drawn from the same entity.  When a pattern references collections other than a
`TribleSet`, ensure the collection implements
[`TriblePattern`](triblespace::core::query::TriblePattern) so that the macro can materialize
the requested triples.

To share a hidden binding across multiple patterns, declare it once with
`temp!` and reference it with `?name` from each clause:

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     attributes! {
#         "A19EC1D9DD534BA9896223A457A6B9C9" as name: inlineencodings::ShortString;
#         "C21DE0AA5BA3446AB886C9640BA60244" as friend: inlineencodings::GenId;
#     }
# }
let mut kb = TribleSet::new();
let alice = ufoid();
let bob = ufoid();
kb += entity! { &alice @ social::name: "Alice", social::friend: &bob };
kb += entity! { &bob @ social::name: "Bob" };

let results: Vec<_> = find!((friend_name: Inline<_>),
    temp!((friend),
          and!(pattern!(&kb, [{ _?person @ social::friend: ?friend,
                                  social::name: ?friend_name }]),
               pattern!(&kb, [{ ?friend @ social::name: "Bob" }]))))
.collect();
```

The `_?person` variable is still local to the first pattern, while `friend`
joins the two constraints without changing the projected results. As above,
`social` denotes a namespace that defines the `name` and `friend` attributes.

## `exists!`

Sometimes you only want to check whether a constraint has any solutions.  The
`exists!` macro mirrors the `find!` syntax but returns a boolean:

```rust,ignore
use triblespace::prelude::*;

assert!(exists!((x), x.is(1.into())));
assert!(!exists!((x), and!(x.is(1.into()), x.is(2.into()))));
```

Internally, `exists!` stops as soon as the first result is found.  It is a
lightweight alternative to `find!` when the mere existence of a match matters
more than the actual bindings.

## Custom constraints

Every building block implements the
[`Constraint`](triblespace::core::query::Constraint) trait.  You can implement this trait on
your own types to integrate custom data sources or query operators with the
solver. Collections that want to power `pattern!` implement
[`TriblePattern`](triblespace::core::query::TriblePattern) so they can materialize the
entity/attribute/value triples a pattern asks for.  Membership-style helpers
such as `has(...)` work with anything that implements
[`ContainsConstraint`](triblespace::core::query::ContainsConstraint), making it easy to join
against pre-existing indexes, caches, or service clients without copying data
into a [`TribleSet`](triblespace::core::trible::TribleSet).

```rust,ignore
use std::collections::HashSet;

use triblespace::prelude::*;
use triblespace::prelude::inlineencodings::ShortString;
use triblespace::core::query::hashsetconstraint::SetConstraint;

struct ExternalTags<'a> {
    tags: &'a HashSet<String>,
}

impl<'a> ContainsConstraint<'a, ShortString> for ExternalTags<'a> {
    type Constraint = SetConstraint<ShortString, &'a HashSet<String>, String>;

    fn has(self, variable: Variable<ShortString>) -> Self::Constraint {
        SetConstraint::new(variable, self.tags)
    }
}

let tags: HashSet<String> = ["rust", "datalog"].into_iter().map(String::from).collect();
let external = ExternalTags { tags: &tags };
let matches: Vec<_> =
    find!((tag: Inline<ShortString>), external.has(tag)).collect();
```

The example wraps an external `HashSet` so it can be queried directly.  A
`TriblePattern` implementation follows the same shape: create a constraint
type that reads from your backing store and return it from `pattern`.  The query
engine drives both traits through `Constraint`, so any data source that speaks
the block-native protocol can participate in `find!`. The six core methods are:

| Method | Role |
|---|---|
| `variables` | Declare the variables touched by the constraint. |
| `estimate` | Append one candidate-count estimate per input row. |
| `propose` | Fill an initially empty sink with candidate extensions. |
| `confirm` | Filter candidates proposed by another constraint without adding any. |
| `satisfied` | Report exact truth once every relevant variable is bound. |
| `influence` | Name estimates that may change after a variable is bound or unbound. |

The explicit `Query::sequential()` scheduler calls these methods with a one-row
[`RowsView`](triblespace::core::query::RowsView) and scalar/plain-value sinks;
every live fresh ordinary iterator uses canonical residual states, while
`Query::lazy_dag_scheduler()` selects the bound-variable-set DAG explicitly for
comparison. Both block-native engines call the same methods with row blocks and
tagged candidate frontiers. Implementations without a
specialized batch operation can loop over the rows, use
`CandidateSink::extend_row`, and use the `confirm_per_row` adapter.

The four row-taking operations must also be row-homomorphic: evaluating
non-empty consecutive sub-blocks independently and concatenating their
row-remapped outputs must equal evaluating the original block. The blocked
schedulers, including the explicit `Query::into_par_dag_iter()` and
`Query::into_par_residual_state_iter()` frontier-sharding paths, are free to
change block boundaries; block-global top-k or first-row semantics would
therefore be incorrect. Diagnostics may observe call boundaries, but must not
feed those observations back into protocol answers. Ordinary `into_par_iter()`
retains the scalar DFS splitter for CPU-oriented workloads.

`propose` owns the empty sink it receives, whereas `confirm` may only remove
entries from an existing sink. `satisfied` may conservatively return `true`
while a relevant variable remains unbound, but its result must be exact once
all of the constraint's variables are present in the view. That exactness is
required for sound composition with `or!` and for constant, zero-variable
checks; it is not merely an optional early-pruning optimization. Zero-variable
roots are settled once during construction. The [Query Engine](query-engine.md#the-constraint-protocol)
chapter explains the protocol and its schedulers in detail.

## Regular path queries

Sometimes you need to traverse a graph without knowing how many hops are
involved. "Find everyone reachable through a chain of `follows` edges" or
"find all ancestors via repeated `parent` links" are naturally recursive — they
cannot be expressed with a fixed number of pattern clauses.

The `path!` macro handles these cases by matching a **regular expression over
edge attributes**. Instead of writing recursive Rust or collecting intermediate
results, you describe the shape of the path and the engine evaluates it:

| Operator | Meaning | Example |
|----------|---------|---------|
| `a` | single edge | `social::follows` |
| `a \| b` | either edge | `follows \| likes` |
| `a b` | concatenation | `follows likes` (follow then like) |
| `a+` | one or more | `follows+` (transitive closure) |
| `a*` | zero or more | `follows*` (reflexive transitive closure) |

`path!` expands to a
[`RegularPathConstraint`](triblespace::core::query::RegularPathConstraint) and composes
with other constraints.  Invoke it through a namespace module
(`social::path!`) to implicitly resolve attribute names:

```rust,ignore
use triblespace::prelude::*;

mod social {
  use triblespace::prelude::*;
  use triblespace::prelude::inlineencodings::*;
  attributes! {
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: GenId;
    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as likes: GenId;
  }
}
let mut kb = TribleSet::new();
let a = fucid(); let b = fucid(); let c = fucid();
kb += entity!{ &a @ social::follows: &b };
kb += entity!{ &b @ social::likes: &c };

let results: Vec<_> = find!((s: Inline<_>, e: Inline<_>),
    path!(&kb, s (social::follows | social::likes)+ e)).collect();
```

You can omit the hex literal in `attributes!` when you only need local or
short‑lived attributes—the macro then derives a deterministic id from the name
and schema via the entity-core mechanism (equivalent to
`Attribute::<S>::from(entity!{ metadata::name: <name handle>, metadata::value_encoding: <S as MetaDescribe>::id() })`).
Stick with explicit ids when the attributes form part of a shared protocol.

The middle section uses a familiar regex syntax to describe allowed edge
sequences.  Editors with Rust macro expansion support provide highlighting and
validation of the regular expression at compile time. Paths reference
attributes from a single namespace; to traverse edges across multiple
namespaces, create a new namespace that re-exports the desired attributes and
invoke `path!` through it.

The endpoints of the path behave like ordinary variables. Bind them in the
`find!` head to join the traversal with additional constraints—for example,
restricting the starting entity or projecting the destination's attributes. If
you want to follow the path but keep one endpoint unprojected, wrap the
traversal in `temp!` so the hidden binding can participate in follow-up
clauses:

```rust,ignore
let interesting_post = fucid();
let influencers = find!((start: Inline<_>),
    temp!((end),
          and!(path!(&kb, start social::follows+ end),
               pattern!(&kb, [{ ?end @ social::likes: interesting_post.to_inline() }]))))
    .collect::<Vec<_>>();
```

Combining `path!` with other constraints like this enables expressive graph
queries while staying in the same declarative framework as the rest of the
chapter.
