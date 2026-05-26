# Macro Cookbook

This chapter is a quick map of the macro surface. The goal is not to replace
the deeper chapters, but to make the everyday question easy:

> "I know roughly what I want to do. Which macro should I reach for?"

The macros fall into three layers:

- **Encoding definition**: `attributes!`
- **Fact construction**: `entity!`
- **Query construction**: `find!`, `exists!`, `pattern!`, `pattern_changes!`,
  `path!`, `and!`, `or!`, `temp!`, `ignore!`

## Define attributes with `attributes!`

Use [`attributes!`](triblespace::core::macros::attributes) to declare typed attributes once,
then reuse them everywhere else.

```rust
use triblespace::prelude::*;

mod social {
    use triblespace::prelude::*;
    use triblespace::prelude::inlineencodings::{GenId, ShortString};

    attributes! {
        /// A person's display name.
        "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;

        /// Another person this person knows.
        pub friend: GenId;
    }
}

assert_ne!(social::name.id(), social::friend.id());
```

Reach for this macro when:

- you are defining a namespace or encoding module
- you want attributes with stable ids and inline encodings
- you want doc comments to become attribute metadata

If you already have attributes, you usually do not need `attributes!` in the
rest of the code you are writing.

## Build facts with `entity!`

Use [`entity!`](triblespace::core::macros::entity) when you want to create tribles for one
entity.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::ShortString;
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#     }
# }
let alice = fucid();
let facts = entity! { &alice @
    social::name: "Alice",
};

assert_eq!(facts.root(), Some(alice.id));
assert_eq!(facts.len(), 1);
```

If you omit the entity id, `entity!` derives one deterministically from the
attribute/value pairs.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::ShortString;
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#     }
# }
let facts = entity! { _
    @ social::name: "Alice"
};

assert!(facts.root().is_some());
assert_eq!(facts.len(), 1);
```

The macro also supports optional and repeated fields:

```rust,ignore
let aliases = ["Al", "A."];
let maybe_nickname = Some("Ace");

let facts = entity! { &alice @
    social::name: "Alice",
    social::nickname?: maybe_nickname,
    social::alias*: aliases,
};
```

Reach for this macro when:

- you are constructing new data
- you want optional/repeated attribute ergonomics
- you want a deterministic derived id for a value object

## Match facts with `pattern!`

Use [`pattern!`](triblespace::core::macros::pattern) to turn trible-shaped structure into a
query constraint.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::{GenId, ShortString};
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#         "B74AA63539354CDA47F387A4C3A8D54C" as pub friend: GenId;
#     }
# }
# let mut kb = TribleSet::new();
# let alice = fucid();
# let bob = fucid();
# kb += entity! { &alice @ social::friend: &bob };
# kb += entity! { &bob @ social::name: "Bob" };
let results: Vec<Id> = find!(
    friend: Id,
    pattern!(&kb, [
        { alice.id @ social::friend: ?friend },
        { ?friend @ social::name: "Bob" }
    ])
).collect();

assert_eq!(results, vec![bob.id]);
```

Inside a pattern:

- `?name` refers to a query variable from the surrounding query
- `_?name` introduces a local helper variable scoped to that pattern
- literal expressions become equality constraints automatically

Use `pattern!` when you are querying the current contents of a `TribleSet`,
`Checkout`, or another pattern-capable source.

## Query for results with `find!`

Use [`find!`](triblespace::core::macros::find) when you want rows back.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::ShortString;
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#     }
# }
# let mut kb = TribleSet::new();
# let alice = fucid();
# kb += entity! { &alice @ social::name: "Alice" };
let names: Vec<Inline<_>> = find!(
    name: Inline<_>,
    pattern!(&kb, [{ _?person @ social::name: ?name }])
).collect();

let first: &str = names[0].try_from_inline().unwrap();
assert_eq!(first, "Alice");
```

There are three common shapes:

- `find!(value, constraint)` for one projected variable as a bare value
- `find!((a, b), constraint)` for tuples
- `find!((), constraint)` when you care only that matches exist

Typed projections happen in the head:

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::ShortString;
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#     }
# }
# let mut kb = TribleSet::new();
# let alice = fucid();
# kb += entity! { &alice @ social::name: "Alice" };
let ids: Vec<_> = find!(
    person: Id,
    pattern!(&kb, [{ ?person @ social::name: "Alice" }])
).collect();

assert_eq!(ids, vec![alice.id]);
```

Use `?` on a projected variable when you want conversion failures as
`Result<T, E>` instead of dropping the row.

## Ask existence questions with `exists!`

Use [`exists!`](triblespace::core::prelude::exists) when you only need yes/no.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     use triblespace::prelude::inlineencodings::ShortString;
#     attributes! {
#         "A74AA63539354CDA47F387A4C3A8D54C" as pub name: ShortString;
#     }
# }
# let mut kb = TribleSet::new();
# let bob = fucid();
# kb += entity! { &bob @ social::name: "Bob" };
let has_bob = exists!(
    pattern!(&kb, [{ _?person @ social::name: "Bob" }])
);

assert!(has_bob);
```

You can also keep the typed-head form when the projection itself matters to the
check:

```rust,ignore
let has_name = exists!(
    (name: Inline<_>),
    pattern!(&kb, [{ ?person @ social::name: ?name }])
);
```

Use `exists!(constraint)` for pure existence checks instead of
`find!((), constraint).next().is_some()`.

## Match only new results with `pattern_changes!`

Use [`pattern_changes!`](triblespace::core::macros::pattern_changes) for incremental
queries: matches are allowed to join against the full current state, but at
least one contributing trible must come from the change set.

```rust,ignore
for (work,) in find!(
    (work: Inline<_>),
    pattern_changes!(&full, &delta, [
        { ?work @ literature::author: &shakespeare }
    ])
) {
    // process only newly introduced matches
}
```

Reach for this macro when:

- you already have `full` and `delta`
- you want monotonic incremental processing
- `pattern!` would re-emit old matches every time

See [Incremental Queries](incremental-queries.md) for the full workflow.

## Traverse edges with `path!`

Use [`path!`](triblespace::core::macros::path) when a relationship is recursive or
variable-length.

```rust
# use triblespace::prelude::*;
# mod social {
#     use triblespace::prelude::*;
#     attributes! {
#         "B74AA63539354CDA47F387A4C3A8D54C" as pub friend: inlineencodings::GenId;
#     }
# }
# let mut kb = TribleSet::new();
# let alice = fucid();
# let bob = fucid();
# let carol = fucid();
# kb += entity! { &alice @ social::friend: &bob };
# kb += entity! { &bob @ social::friend: &carol };
let results: Vec<(Id, Id)> = find!(
    (src: Id, dst: Id),
    path!(kb.clone(), src social::friend+ dst)
).collect();

assert!(results.contains(&(alice.id, bob.id)));
assert!(results.contains(&(alice.id, carol.id)));
assert!(results.contains(&(bob.id, carol.id)));
```

The middle part is a small regular language over attributes:

- adjacency means concatenation
- `|` means alternation
- `*` means zero or more
- `+` means one or more
- `?` means zero or one
- `^p` reverses the direction of an attribute (or path-element) —
  `^p` followed by `?`/`*`/`+` binds the modifier *inside* the
  inversion: `^p+` is `^(p+)`, matching SPARQL 1.1 §17.5
- `!p` matches any attribute *other than* `p` (negated
  property set, single-attribute form)
- parentheses group

Example:

```rust,ignore
path!(kb.clone(), start (social::friend | social::colleague)+ end)
```

Use `path!` when a fixed number of `pattern!` clauses would be awkward or
impossible.

## Combine constraints with `and!` and `or!`

Use [`and!`](triblespace::core::prelude::and) when every clause must hold:

```rust,ignore
find!(
    (friend: Inline<_>),
    and!(
        pattern!(&kb, [{ ?person @ social::name: "Alice" }]),
        pattern!(&kb, [{ ?person @ social::friend: ?friend }])
    )
)
```

Use [`or!`](triblespace::core::prelude::or) when any branch may hold:

```rust,ignore
find!(
    (alias: Inline<_>),
    or!(
        pattern!(&kb, [{ ?person @ social::nickname: ?alias }]),
        pattern!(&kb, [{ ?person @ social::name: ?alias }])
    )
)
```

`or!` branches must mention the same variable set.

## Introduce helpers with `temp!`

Use [`temp!`](triblespace::core::prelude::temp) when you need a fresh variable only inside a
sub-expression.

```rust,ignore
find!(
    (person: Inline<_>),
    temp!((friend), and!(
        pattern!(&kb, [{ ?person @ social::friend: ?friend }]),
        pattern!(&kb, [{ ?friend @ social::name: "Bob" }])
    ))
)
```

This is useful when the helper participates in joins but should not be
projected.

## Hide helpers with `ignore!`

Use [`ignore!`](triblespace::core::prelude::ignore) when a constraint needs internal
variables that should not be visible to the outer planner.

```rust,ignore
find!(
    (person: Inline<_>),
    ignore!((friend),
        pattern!(&kb, [
            { ?person @ social::friend: ?friend },
            { ?friend @ social::name: "Bob" }
        ])
    )
)
```

This is a more specialized tool than `temp!`. Reach for it when you already
have a nested constraint that uses helper variables internally and you want to
hide them from the outer query.

## Which macro should I use?

If you are:

- defining encodings: use `attributes!`
- building facts for one entity: use `entity!`
- matching trible structure: use `pattern!`
- matching only newly added results: use `pattern_changes!`
- asking for rows back: use `find!`
- asking for a boolean: use `exists!`
- traversing recursive edges: use `path!`
- requiring all clauses: use `and!`
- allowing alternatives: use `or!`
- introducing a fresh helper variable: use `temp!`
- hiding helper variables from the outer query: use `ignore!`

From here, the best next stops are:

- [Query Language](query-language.md) for the execution model
- [Patterns & Recipes](patterns-and-recipes.md) for modeling patterns
- [Incremental Queries](incremental-queries.md) for `pattern_changes!`
