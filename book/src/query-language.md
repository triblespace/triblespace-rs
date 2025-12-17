# Query Language

This chapter introduces the core query facilities provided by `triblespace`.  A
query is described in a small declarative language that states which values
should match instead of spelling out the iteration strategy.  When you read a
query, you are effectively looking at a logical statement about the data: *if*
the constraints can all be satisfied, *then* the variable bindings are produced
as results.  The declarative style gives the engine freedom to reorder work and
choose efficient execution strategies.

Every macro shown here is a convenience wrapper around a concrete
[`Constraint`](crate::query::Constraint) implementation.  When you need finer
control—or want to assemble constraints manually outside the provided
macros—reach for the corresponding builder types in
[`tribles::query`](crate::query).

## Declaring a query

The [`find!`](crate::prelude::find) macro builds a
[`Query`](crate::query::Query) by declaring variables and a constraint
expression. The macro mirrors Datalog syntax: the head `((...))` lists the
variables you want back, and the body describes the conditions they must meet.
A minimal invocation looks like this:

```rust
let results = find!((a), a.is(1.into())).collect::<Vec<_>>();
```

`find!` returns an [`Iterator`](core::iter::Iterator) over tuples of the bound
variables. Matches can be consumed lazily or collected into common
collections:

```rust
for (a,) in find!((a), a.is(1.into())) {
    println!("match: {a}");
}
```

The head tuple supports pattern destructuring, so adding more variables is as
simple as expanding the list: `find!((a, b, c), ...)` yields `(a, b, c)` tuples.
Variables declared in the head can be reused multiple times inside the
constraint to express joins. When a variable appears in several clauses the
engine ensures every occurrence binds to the same value. Repeating a variable in
two patterns, for example, restricts the result set to entities that satisfy
both attribute assignments simultaneously. The order of declarations defines the
shape of the tuple in the iterator, so reorganising the head changes how you
destructure results.

### Typed variables

Variables optionally include a concrete type to convert the underlying value.
The constraint phase still works with untyped [`Value`](crate::value::Value)
instances; conversion happens when the tuple is emitted.  These conversions use
[`FromValue`](crate::value::FromValue) and panic if decoding to the requested
type fails. Bind the variable as a [`Value<_>`](crate::value::Value) and reach
for [`TryFromValue`](crate::value::TryFromValue) yourself when you want to
surface conversion errors instead of panicking.

```rust
find!((x: i32, y: Value<ShortString>),
      and!(x.is(1.into()), y.is("foo".to_value())))
```

The first variable is read as an `i32` and the second as a short string if the
conversion succeeds. The query engine walks all possible assignments that
satisfy the constraint and yields tuples of the declared variables in the order
they appear in the head.

### Collecting results

Any type that implements [`FromIterator`](core::iter::FromIterator) can collect
the results of a query.  `Vec<_>` is common for tests and examples, while
`HashSet<_>` is useful when the match order is irrelevant.  When you only need
the first result, call iterator adapters such as `next`, `find`, or `try_fold`
to avoid materializing the full result set.

## Built-in constraints

`find!` queries combine a small set of constraint operators to form a
declarative language for matching tribles.  Each operator implements
[`Constraint`](crate::query::Constraint) and can therefore be mixed and nested
freely.

| Macro | Purpose | Notes |
| ----- | ------- | ----- |
| [`and!`](crate::prelude::and) | Require every sub-constraint to hold | Builds an [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint). |
| [`or!`](crate::prelude::or) | Accept any satisfied alternative | Produces a [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint) whose branches must reference the same variables. |
| [`ignore!`](crate::ignore) | Drop variables from a sub-query | Removes the listed variables from planning so a constraint can contribute only along the remaining bindings. |
| [`temp!`](crate::temp) | Mint hidden helper variables | Allocates fresh bindings for the nested expression so the helpers can join across patterns without being projected. |
| [`pattern!`](crate::pattern) | Match attribute assignments in a collection | Expands to a [`TriblePattern`](crate::query::TriblePattern)-backed constraint that relates attributes and values for the same entity. |
| [`pattern_changes!`](crate::pattern_changes) | Track attribute updates incrementally | Builds a [`TriblePattern`](crate::query::TriblePattern) constraint that yields newly added triples from a change set because incremental evaluation stays monotonic; see [Incremental Queries](incremental-queries.md) for the broader evaluation workflow. |
| `.is(...)` | Pin a variable to a constant | Wraps a [`ConstantConstraint`](crate::query::constantconstraint::ConstantConstraint) that compares the binding against a literal value. |
| `has` | Check membership in a collection | Collections such as [`HashSet`](std::collections::HashSet) expose `.has(...)` when they implement [`ContainsConstraint`](crate::query::hashsetconstraint::ContainsConstraint); triple stores like [`TribleSet`](crate::tribleset::TribleSet) instead participate through [`pattern!`](crate::pattern). |

Any data structure that can iterate its contents, test membership, and report
its size can implement `ContainsConstraint`. Membership constraints are
particularly handy for single-column collections such as sets or map key views,
while multi-position sources like `TribleSet` rely on `pattern!` to keep entity,
attribute, and value bindings aligned.

### Constant matches (`is`)

Call [`Variable::is`](crate::query::Variable::is) when you need a binding to
equal a specific value.  The method returns a
[`ConstantConstraint`](crate::query::constantconstraint::ConstantConstraint)
that checks whether the solver can assign the variable to the provided
[`Value`](crate::value::Value).  Constant constraints behave like any other
clause: combine them with `and!` to narrow a variable after other constraints
have proposed candidates, or place them inside `or!` branches to accept
multiple literals.

```rust
find!((title: Value<_>),
      and!(dataset.has(title), title.is("Dune".to_value())));
```

The snippet above keeps only the rows where `title` equals `"Dune"`.  Because
`is` constrains the variable's value rather than projecting a new binding, it
is also handy for helpers such as `temp!` when you want to filter hidden
bindings without exposing them in the result tuple.

`pattern!` and `pattern_changes!` construct constant constraints for literal
values automatically, so you often get the same behaviour simply by writing the
desired value in the pattern:

```rust
find!((friend: Value<_>),
      pattern!(&dataset,
               [{ _?person @ social::friend: ?friend,
                  social::city: "Caladan" }]));
```

Repeating `.is(...)` on the same variable with different values causes the
query to fail—just as conflicting `pattern!` clauses would—so prefer `or!` (or
switch to a membership helper such as `.has(...)`) when you want to accept
several constants.

### Intersections (`and!`)

[`and!`](crate::prelude::and) combines multiple constraints that must all hold
simultaneously.  Each sub-clause can introduce new bindings or further narrow
existing ones, and the solver is free to reorder the work to reduce the search
space.  When a sub-constraint fails to produce a candidate that is compatible
with the current bindings, the whole conjunction rejects that branch and moves
on.  The macro accepts any number of arguments, so `and!(...)` is often a
convenient way to keep related clauses together without nesting additional
`find!` calls:

```rust
let favourites = favourite_titles(); // e.g. a HashSet<Id> built elsewhere
find!((book: Value<_>, author: Value<_>),
      and!(favourites.has(book),
           pattern!(&dataset,
                    [{ ?book @ literature::title: "Dune",
                       literature::author: ?author }])));
```

Here the membership test over `favourites` and the attribute pattern from
`dataset` run as part of the same conjunction.  The solver joins them on their
shared bindings (`book` and `author`) so only tuples that satisfy every clause
make it into the result set.  Because `and!` simply returns a constraint, you
can nest it inside other combinators such as `temp!`, `ignore!`, or `or!` to
structure queries however you like.

### Alternatives (`or!`)

Use [`or!`](crate::prelude::or) to express alternatives. Each branch behaves
like an independent constraint and may introduce additional bindings that
participate in the surrounding query, provided every branch mentions the same
set of variables:

```rust
find!((alias: Value<_>),
      temp!((entity),
            or!(pattern!(&dataset,
                         [{ ?entity @ profile::nickname: ?alias }]),
                pattern!(&dataset,
                         [{ ?entity @ profile::display_name: ?alias }]))));
```

Each branch contributes every match it can produce given the current bindings.
In the example above, people who have both a nickname and a display name yield
two rows—one for each attribute—because the solver keeps the union of all
solutions to preserve the query's monotonic semantics. Branches that cannot
match simply contribute nothing. Because each branch is still a full constraint,
combine `or!` with `temp!` when you need hidden helpers or wrap portions in
`ignore!` to drop positions that do not matter for a particular alternative. If
two branches reference different variables the macro panics at runtime, so keep
the variable sets aligned even when some branches ignore portions of a
constraint.

### Ignoring bindings (ignore!)

Ignored variables are handy when a sub-expression references fields you want to
drop. The [`IgnoreConstraint`](crate::query::ignore::IgnoreConstraint)
subtracts the listed bindings from the constraint's
[`VariableSet`](crate::query::VariableSet), so the planner never attempts to
join them with the outer query, project them into the results, or even solve
for those positions. From the solver's perspective those slots vanish
completely—it keeps evaluating the remaining bindings while treating the
ignored ones as don't-care wildcards. Triple-based constraints, for example,
always propose entity/attribute/value combinations; wrapping them in
`ignore!((value), ...)` continues to constrain the entity and attribute while
discarding the value column entirely. Clauses that reference at least one
surviving variable still run and continue to narrow those bindings. If a branch
mentions only ignored variables there is nothing left to relate to the outer
query, so the planner has no variable along which to schedule it; the inner
constraint is never consulted and the expression behaves as though it were
omitted.

The identifiers you list inside `ignore!` expand to fresh bindings scoped to
the nested expression, but subtracting them from the outer plan means the solver
never unifies those bindings—or even asks the constraint to propose values for
them. Even if you repeat the same name across multiple clauses, each occurrence
behaves like an independent wildcard. Reach for [`temp!`](crate::temp) when you
want a hidden variable to participate in the surrounding plan without being
projected; reach for `ignore!` when you want to use a multi-column constraint
while only keeping some of its positions.

This inertness is a strict scoping rule rather than existential quantification.
If you need to assert that some related fact exists without reifying its value,
structure the pattern so the surviving variables capture that dependency. The
macro automatically uses the ambient context that `find!` or `matches!`
provides, so typical invocations only specify the variable list and nested
constraint:

```rust
find!((person: Value<_>),
      ignore!((street_value),
              pattern!(&dataset, [{ ?person @ contacts::street: ?street_value }])));
```

Here the pattern still constrains `person` because the triple ties the entity to
an attribute, yet the actual street string is ignored. Had the block mentioned
only `street_value`, the entire expression would have evaporated—there would be
no remaining link to the rest of the query—so the outer query would not learn
anything. Reusing `street_value` elsewhere in the ignored expression also does
not force the sub-clauses to agree, because each occurrence is treated as its
own wildcard; introduce a `temp!` binding when you need the same hidden value to
appear in multiple places.

### Temporary variables (temp!)

Real queries often need helper bindings that participate in the joins but do
not show up in the result tuple. Wrap the relevant constraint with
`temp!((...vars...), expr)` to mint hidden variables and evaluate `expr` with
them in scope:

```rust
find!((person: Value<_>),
      temp!((friend),
            and!(pattern!(&dataset,
                          [{ _?p @ social::person: ?person, social::friend: ?friend }]),
                 pattern!(&dataset,
                          [{ ?friend @ social::city: "Caladan" }]))));
```

The helper binding `friend` links the two patterns, ensuring the same entity is
used across both clauses without expanding the result tuple. `temp!` can create
multiple variables at once (`temp!((street, city), ...)`). Like `ignore!`, you
always wrap the hidden bindings in a tuple, so each invocation reads
`temp!((...vars...), ...)`. Here `social` would be a namespace module exporting
the `person`, `friend`, and `city` attributes. The variables adopt the value
schemas implied by the constraints they appear in, so no extra annotations are
required. When working outside the query macros, call
[`VariableContext::next_variable`](crate::query::VariableContext::next_variable)
directly instead.

When the helper variable lives entirely within a single pattern, consider using
`_?alias` instead of `temp!`. Both [`pattern!`](crate::pattern) and
[`pattern_changes!`](crate::pattern_changes) support `_?ident` placeholders that
mint fresh bindings scoped to that one macro invocation. They behave like
non-projected variables: you can reuse the `_?ident` multiple times inside the
pattern to relate attributes, but the binding vanishes once control leaves the
macro. Reach for `temp!` when the helper must span several constraints or when
you need to reuse the same hidden binding across multiple patterns.

## Example

```rust
use triblespace::prelude::*;
use triblespace::examples::{self, literature};

let dataset = examples::dataset();

for (title,) in find!((title: Value<_>),
                     and!(dataset.has(title), title.is("Dune".to_value()))) {
    println!("Found {}", title.from_value::<&str>());
}
```

This query searches the example dataset for the book titled "Dune".  The
variables and constraint can be adapted to express more complex joins and
filters.  For instance, you can introduce additional variables to retrieve both
the title and the author while sharing the same dataset predicate:

```rust
for (title, author) in find!((title: Value<_>, author: Value<_>),
                             and!(title.is("Dune".to_value()),
                                  pattern!(&dataset,
                                           [{ _?book @ literature::title: ?title,
                                              literature::author: ?author }]))) {
    println!("{title} was written by {}", author.from_value::<&str>());
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
let mut kb = TribleSet::new();
let e = ufoid();
kb += entity! { &e @ literature::firstname: "Frank", literature::lastname: "Herbert" };

let author_last_names: Vec<_> = find!((last: Value<_>),
    pattern!(&kb, [{ _?person @ literature::firstname: "Frank", literature::lastname: ?last }])
).collect();
```

Here `_?person` remains scoped to the pattern while ensuring both attributes are
drawn from the same entity.  When a pattern references collections other than a
`TribleSet`, ensure the collection implements
[`TriblePattern`](crate::query::TriblePattern) so that the macro can materialize
the requested triples.

To share a hidden binding across multiple patterns, declare it once with
`temp!` and reference it with `?name` from each clause:

```rust
let mut kb = TribleSet::new();
let alice = ufoid();
let bob = ufoid();
kb += entity! { &alice @ social::name: "Alice", social::friend: &bob };
kb += entity! { &bob @ social::name: "Bob" };

let results: Vec<_> = find!((friend_name: Value<_>),
    temp!((friend),
          and!(pattern!(&kb, [{ _?person @ social::friend: ?friend,
                                  social::name: ?friend_name }]),
               pattern!(&kb, [{ ?friend @ social::name: "Bob" }]))))
.collect();
```

The `_?person` variable is still local to the first pattern, while `friend`
joins the two constraints without changing the projected results. As above,
`social` denotes a namespace that defines the `name` and `friend` attributes.

## `matches!`

Sometimes you only want to check whether a constraint has any solutions.  The
`matches!` macro mirrors the `find!` syntax but returns a boolean:

```rust
use triblespace::prelude::*;

assert!(matches!((x), x.is(1.into())));
assert!(!matches!((x), and!(x.is(1.into()), x.is(2.into()))));
```

Internally, `matches!` stops as soon as the first result is found.  It is a
lightweight alternative to `find!` when the mere existence of a match matters
more than the actual bindings.

## Custom constraints

Every building block implements the
[`Constraint`](crate::query::Constraint) trait.  You can implement this trait on
your own types to integrate custom data sources or query operators with the
solver. Collections that want to power `pattern!` implement
[`TriblePattern`](crate::query::TriblePattern) so they can materialize the
entity/attribute/value triples a pattern asks for.  Membership-style helpers
such as `has(...)` work with anything that implements
[`ContainsConstraint`](crate::query::ContainsConstraint), making it easy to join
against pre-existing indexes, caches, or service clients without copying data
into a [`TribleSet`](crate::trible::TribleSet).

```rust
use std::collections::HashSet;

use triblespace::prelude::*;
use triblespace::prelude::valueschemas::ShortString;
use triblespace::query::hashsetconstraint::SetConstraint;

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
    find!((tag: Value<ShortString>), external.has(tag)).collect();
```

The example wraps an external `HashSet` so it can be queried directly.  A
`TriblePattern` implementation follows the same shape: create a constraint
type that reads from your backing store and return it from `pattern`.  The query
engine drives both traits through `Constraint`, so any data source that can
estimate, propose, and confirm candidate values can participate in `find!`.

## Regular path queries

The `path!` macro lets you search for graph paths matching a regular
expression over edge attributes.  It expands to a
[`RegularPathConstraint`](crate::query::RegularPathConstraint) and can be
combined with other constraints.  Invoke it through a namespace module
(`social::path!`) to implicitly resolve attribute names:

```rust
use triblespace::prelude::*;

mod social {
  use triblespace::prelude::*;
  use triblespace::prelude::valueschemas::*;
  attributes! {
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: GenId;
    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as likes: GenId;
  }
}
let mut kb = TribleSet::new();
let a = fucid(); let b = fucid(); let c = fucid();
kb += entity!{ &a @ social::follows: &b };
kb += entity!{ &b @ social::likes: &c };

let results: Vec<_> = find!((s: Value<_>, e: Value<_>),
    path!(&kb, s (social::follows | social::likes)+ e)).collect();
```

You can omit the hex literal in `attributes!` when you only need local or
short‑lived attributes—the macro then derives a deterministic id from the name
and schema just like `Attribute::from_name`. Stick with explicit ids when the
attributes form part of a shared protocol.

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

```rust
let interesting_post = fucid();
let influencers = find!((start: Value<_>),
    temp!((end),
          and!(path!(&kb, start social::follows+ end),
               pattern!(&kb, [{ ?end @ social::likes: interesting_post.to_value() }]))))
    .collect::<Vec<_>>();
```

Combining `path!` with other constraints like this enables expressive graph
queries while staying in the same declarative framework as the rest of the
chapter.

## Learn more

- The [Query Engine](query-engine.md) chapter shows how the solver orders
  constraints, estimates work, and plans the joins described here.
- [Atreides Join](atreides-join.md) dives into the execution strategy behind the
  query macros when they evaluate against large trible sets.
- The [Trible Structure](deep-dive/trible-structure.md) deep dive visualizes the
  storage layout and indexing model that query constraints read from.
