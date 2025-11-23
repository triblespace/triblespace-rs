# Query Engine

Queries describe the patterns you want to retrieve. The engine favors extreme
simplicity and aims for predictable latency and skew resistance without any
tuning. Every constraint implements the
[`Constraint`](crate::query::Constraint) trait so operators, sub-languages, and
even alternative data sources can compose cleanly. Query evaluation is
expressed as a negotiation between constraints, but the contract stays tiny:
constraints report which variables they touch, estimate how many candidates
remain for each variable, and can enumerate concrete values on demand. Those
estimates feed directly into the depth-first search; there is no standalone
planner to build or cache.

The constraint API mirrors the mental model you would use when reasoning about
a query by hand. Constraints expose their
[`VariableSet`](crate::query::VariableSet), provide `estimate` methods so the
engine can choose the next variable to bind, and implement `propose` to extend
partial assignments. Composite constraints, such as unions, may also override
`confirm` to tighten previously gathered proposals. This cooperative protocol
keeps the engine agnostic to where the data comes from—be it in-memory
indices, remote stores, or bespoke application predicates.

## Search Loop

The engine executes a query as a depth-first search over partial bindings. The
loop integrates the cardinality heuristics directly instead of running a
separate planning phase. Picking a different heuristic simply yields another
member of the Atreides join family:

1. **Initialisation** – When a [`Query`](crate::query::Query) is constructed it
   asks the root constraint for its variable set. The engine records each
   variable's [`influence`](crate::query::Constraint::influence) so it knows
   which estimates to refresh when bindings change, computes an initial
   `estimate` for every variable, and sorts the yet-unbound list using those
   numbers.
2. **Propose (and confirm)** – The engine pops the most selective variable from
   the sorted list and calls `propose` to collect candidate values that respect
   the current binding. Intersection constraints such as those built by
   [`and!`](crate::prelude::and) reorder their children by increasing estimate
   and call `confirm` on the remaining branches so inconsistent candidates are
   filtered out before the engine commits to them. Constraints that observe the
   partial assignment simply avoid proposing or confirming values they already
   know will fail.
3. **Bind or Backtrack** – If `propose` yielded values, the engine binds one,
   marks that variable as touched, and recurses. If no candidates remain it
   backtracks: the binding is undone, the variable returns to the unbound set,
   and the touched marker ensures dependent estimates refresh before the next
   attempt.
4. **Yield** – Once every variable is bound the post-processing closure runs
   and the tuple is produced as a result row.

Between iterations the engine refreshes estimates for any variables influenced
by the newly bound values. Because constraints only observe the bindings they
explicitly depend on, unrelated sub-queries interleave naturally and new
constraint types can participate without custom planner hooks.

## Queries as Schemas

You might notice that trible.space does not define a global ontology or schema
beyond associating attributes with a
[`ValueSchema`](crate::value::ValueSchema) or
[`BlobSchema`](crate::prelude::BlobSchema). This is deliberate. The semantic web
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
through `Constraint::estimate` to guide the depth-first search over variable
bindings, providing skew-resistant and predictable performance. Because the
engine refreshes those estimates inside the search loop, the binding order
adapts whenever a constraint updates its influence set—there is no separate
planning artifact to maintain. For a detailed discussion, see the [Atreides
Join](atreides-join.md)
chapter.

## Query Languages

Instead of a single query language, the engine exposes small composable
constraints that combine with logical operators such as `and` and `or`. These
constraints are simple yet flexible, enabling a wide variety of operators while
still allowing the engine to explore the search space efficiently.

The query engine and data model are flexible enough to support many query
styles, including graph, relational and document-oriented queries. Constraints
may originate from the database itself (such as attribute lookups), from custom
application logic, or from entirely external sources.

For example, the [`pattern!`](crate::pattern!) and
[`entity!`](crate::entity!) macros—available at the crate root and re-exported
via [`triblespace::prelude`](crate::prelude) (for instance with
`use triblespace::prelude::*;`)—generate constraints for a given trible pattern in
a query-by-example style reminiscent of SPARQL or GraphQL but tailored to a
document-graph data model. It would also be possible to layer a property-graph
language like Cypher or a relational language like Datalog on top of the
engine.[^3]

```rust
use std::collections::HashSet;

use tribles::examples::literature;
use tribles::prelude::*;
use tribles::prelude::valueschemas::ShortString;

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

    let mut allowed = HashSet::<Value<ShortString>>::new();
    allowed.insert("Frank".to_value());

    let results: Vec<_> = find!((title: Value<_>, firstname: Value<_>),
        and!(
            allowed.has(firstname),
            pattern!(&kb, [{
                ?person @
                    literature::firstname: ?firstname,
                    literature::lastname: "Herbert",
            }, {
                literature::author: ?person,
                literature::title: ?title,
            }])
        )
    )
    .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "Dune".to_value());
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
