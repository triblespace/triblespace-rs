# Descriptive Structural Typing and the find! Idiom

This chapter documents the mental model and idioms we recommend when working
with tribles. The model is intentionally descriptive: queries declare the
shape of the data you want to see rather than prescribing a single concrete
Rust type for every entity. This gives you the flexibility to keep the full
graph around and to materialize only the view you need, when you need it.

Reading the chapter sequentially should equip you to:

1. talk about entities in terms of the fields they expose rather than the
   structs you wish they were,
2. design APIs that carry just enough information (a workspace, a checkout, an
   id) to ask for more data later, and
3. know when it is worth materializing a typed projection and when the
   descriptive view is already the best representation.

## Key ideas at a glance

- Attributes are typed fields (unlike untyped RDF predicates).
- An entity in tribles is a structural record: a bag of typed fields.
- `attributes!` accepts explicit ids for shared columns and can derive
  deterministic ids from a name + schema when you omit the hex literal; use the
  derived form for quick, local attributes and reserve explicit ids for
  published protocols.
- find! patterns are descriptive type checks / projections: they select
  entities that match a requested shape.
- entity! constructs ad‑hoc entities (like struct literals).
- Reified kinds/tags are attached via metadata::tag (GenId); projects often
  export canonical KIND_* constants you can pattern-match directly against.
- Value schema entities are tagged with metadata::KIND_VALUE_SCHEMA; blob schema
  entities with metadata::KIND_BLOB_SCHEMA.
- Prefer passing the Workspace + the checkout result (TribleSet) and an
  entity id around — only materialize a concrete Rust view when required.
- Strongly prefer operating on the tuples returned by `find!`; wrapper
  structs should exist only as grudging adapters for APIs that demand them.

## Why "descriptive" not "prescriptive"?

In a prescriptive system you define a named struct (type), commit to it, and
force conversions at boundaries. Tribles instead let you describe the fields
you need at call sites. That keeps code resilient to schema evolution and
avoids unnecessary unfolding of the graph.

In linguistic terms: instead of insisting every entity be declared as
CategoryX, you ask "show me entities that have fields A and B" and work with
those. If an entity also has field C that's fine — it simply matches the
descriptive pattern.

## Type theory mapping (short)

- Structural typing: types are shapes of fields, not names.
- Width subtyping: records with more fields subsume records with fewer.
- Intersection types: requiring both patterns A and B is like A & B.
- Row polymorphism: patterns naturally allow additional (unspecified)
  fields to exist.

## Core idioms and recommended patterns

### 1. Use Workspace as your core I/O handle

The Workspace is the primary object for interacting with a repository. It
lets you open a branch, commit, push, checkout history, and — importantly —
read blob handles (LongString) cheaply.

Pattern: open a workspace for the configured branch, checkout the HEAD
ancestors to produce a TribleSet content snapshot for efficient read-only
pattern matching, and use the same Workspace to lazily read blobs when you
need them.

This avoids duplicating memory and allows cheap zero-copy access to LongString
blobs.

#### Manager-owned repository and workspace DI

At runtime, prefer to give a long-lived manager (session, exporter, service)
ownership of a `Repository<Pile<Blake3>>`. Downstream code can depend on that
manager in one of two shapes:

1. Accept a `&mut Repository<_>` and open/pull the workspace you need inside
   the function. This works well for tasks that need to coordinate multiple
   checkouts or want to control the retry loop themselves, and the mutable
   borrow is typically short-lived: you only need it while creating or
   pushing workspaces.
2. Ask the manager to mint a `&mut Workspace<_>` for the duration of a task
   (e.g. an update, render, or event-handling callback) and pass that mutable
   reference down. The manager remains responsible for merging or dropping the
   workspace when the task completes.

Both approaches avoid constructing piles or repositories ad-hoc and keep
lifecycle management centralized. Importantly, they let multiple tasks hold
distinct mutable workspaces over the same repository while only requiring a
single mutable borrow of the repository when you create or push those
workspaces. Library functions should therefore accept a `&mut Repository<_>`
*or* a `&mut Workspace<_>` (optionally paired with a `TribleSet` checkout for
read-only helpers) rather than opening new repositories inside hot paths.

Example (pseudocode):

```rust
// manager owns a Repository for the process/session lifetime
let mut repo = manager.repo_mut();
let branch_id = manager.default_branch_id;

// option 1: task pulls its own workspace
let mut ws = repo.pull(branch_id)?;
let content = ws.checkout(ws.head())?;

// option 2: manager provides a workspace to a task callback
manager.with_workspace(branch_id, |ws| {
    let snapshot = ws.checkout(ws.head())?;
    render(snapshot);
    Ok(())
})?;
```

This pattern keeps startup/teardown centralized and eliminates the common
hot-loop anti-pattern of repeatedly opening ephemeral repository instances.

### 2. Use find! as a descriptive type / projection

find! is not just a query language; it is the place where you declare the
shape of the data you expect. Treat find! patterns as lightweight, inline
type declarations. If an entity doesn't match, find! won't return it — no
error, just absence.

When your project defines canonical tag ids (GenId constants) prefer to
match the tag directly in the pattern rather than binding a short-string
and filtering afterwards. Pattern clauses can be composed: you can match on
tags and required attributes, and even join related entities in a single
find! invocation.

Example: find plan snapshot ids (match tag directly)

```rust
// Match entities that have the canonical plan snapshot tag attached.
for (e,) in find!((e: Id), triblespace::pattern!(&content, [{ ?e @ metadata::tag: (KIND_PLAN_SNAPSHOT) }])) {
    // `e` is a plan snapshot entity id; follow-up finds can read other fields
}
```

Worked example: composing a structural pattern

```rust
// Grab all active plans with a title and owner.
for (plan_id, title, owner) in find!(
    (plan_id: Id, title: ShortString, owner: Id),
    tribles::pattern!(&content, [
        { ?plan_id @ metadata::tag: (KIND_PLAN) },
        { ?plan_id plan::status: (plan::STATUS_ACTIVE) },
        { ?plan_id plan::title: ?title },
        { ?plan_id plan::owner: ?owner },
    ])
) {
    // `title` is already typed as ShortString.
    // `owner` can drive a follow-up find! to pull account info as needed.
}
```

`find!` returns tuples of the values you requested in the head of the query.
Nothing more, nothing less. The example above reads as "give me the `Id`
bound to `plan_id`, the short string bound to `title`, and another `Id`
bound to `owner` for every entity matching these clauses." Because the
matching is descriptive, adding a new attribute such as `plan::color` does
not invalidate the call site — you only see the data you asked for.

### 3. Lazy, ad‑hoc conversions only where needed

If a function needs a few fields for an operation, ask for them with find!
inside the function. If later you perform an operation that needs different
fields, you can perform another small find! there. Don't materialize large
subgraphs unless a single operation needs them.

The recommended function signature is minimal and focused on the
tribles primitives. Conversions into bespoke structs are almost always a smell;
they obscure which fields are actually used and quickly devolve into an
unofficial schema. Treat any adapter as an opt-in shim that exists purely at
integration boundaries where consumers refuse to speak tribles primitives.

```rust
fn handle_plan_update(ws: &mut Workspace<Pile<Blake3>>, plan_id: Id) -> io::Result<()> {
    // ad-hoc find! calls to read the fields we need
    let checkout = ws.checkout()?;

    if let Some((title,)) =
        find!((title: ShortString), tribles::pattern!(&checkout, [{ ?plan_id plan::title: ?title }]))
            .next()
    {
        // The returned tuple is already typed. Convert to an owned String only if
        // external APIs demand ownership.
        process_title(title.from_value::<&str>());
    }

    Ok(())
}
```

If you cannot avoid exposing a typed facade (for example because an external
API insists on receiving a struct), keep the struct tiny, document that it is a
legacy shim, and derive it straight from a find! tuple:

```rust
struct PlanSummary<'a> {
    id: Id,
    title: &'a str,
}

fn load_plan_summary<'a>(ws: &'a mut Workspace<Pile<Blake3>>, plan_id: Id) -> io::Result<Option<PlanSummary<'a>>> {
    let content = ws.checkout()?;
    Ok(find!(
        (title: ShortString),
        tribles::pattern!(&content, [{ ?plan_id plan::title: ?title }])
    )
    .next()
    .map(|(title,)| PlanSummary {
        id: plan_id,
        title: title.from_value::<&str>(),
    }))
}
```

The struct above is merely a view with borrowed references; it is not a
blessed schema. Resist the temptation to evolve it into a "real" model type —
extend the `find!` pattern first and only mirror those changes here when an
external interface forces your hand.

### 4. Read LongString as &str (zero-copy)

Blob schema types in tribles are intentionally zerocopy. Prefer the
typed View API which returns a borrowed &str without copying when possible.

```rust
let view = ws
    .get::<View<str>, LongString>(handle)
    .map_err(|e| ...)?; // `handle` is a Value<Handle<Blake3, LongString>>
let s: &str = view.as_ref(); // zero-copy view tied to the workspace lifetime
// If you need an owned String: let owned = view.to_string();
```

Note: a View borrows data that is managed by the Workspace; avoid returning
`&str` that outlives the workspace or the View.

When you do need ownership, convert at the edge of the system. Internal
helpers should continue to pass views or typed handles around so that the
call site that triggers a blob fetch is easy to spot. Cloning the in-memory
blob after it has been pulled is cheap; the expensive part is fetching it
from storage (potentially a remote) in the first place.

### 5. Structural sharing and normalization patterns

When persisting graphs that contain many repeated or immutable pieces
(e.g. steps in a plan), prefer structural sharing:
- Store canonical step entities (LongString blobs for their text).
- Create a lightweight "link" entity per plan that references the step ids
  and metadata like order and status.

On update, create new step entities only for truly new step text and add a
new snapshot entity that references the steps. This keeps history immutable
and easy to reason about.

### 6. Push/merge retry loop for writers

When pushing writes, use the standard push/merge loop to handle concurrent
writers. Two options are available:

- Manual conflict handling with `try_push` (single attempt; returns a
  conflicting workspace on CAS failure):

```rust
ws.commit(content, Some("plan-update"));
let mut current_ws = ws;
while let Some(mut incoming) = repo.try_push(&mut current_ws)? {
    incoming.merge(&mut current_ws)?;
    current_ws = incoming;
}
```

- Automatic retries with `push` (convenience wrapper that merges and retries
  until success or error):

```rust
ws.commit(content, Some("plan-update"));
// `push` will handle merge+retry internally; it returns Ok(()) on success
// or an error if the operation ultimately failed.
repo.push(&mut ws)?;
```

## Practical anti‑patterns
- Do not unfold the graph or convert it into nested Rust structs.
  It wastes CPU and memory and loses the benefits of tribles’ flexible
  reifications.
- Avoid holding repo locks across async/await points. Acquire workspaces,
  do the minimal synchronous I/O you need, then release locks before awaiting.
- Don’t assume presence of a field; be explicit about optional vs required
  semantics using Option / Result in typed adapters.
- Don't create ephemeral Repository instances every time you need to read or
  write data. Instead, own a long-lived Repository in a manager and expose
  workspace-opening helpers.
- Don't explicitly convert Values via from_value/to_value; use typed `find!`
  patterns `find!((field: <field_type>), ...)` and
  `ws.get::<View<...>, _>(handle)` for blob reads.
- Don't create helper functions for queries, use `find!` patterns directly in
  the function that needs them. This keeps the shape of the data explicit and
  close to where it is used and avoids unnecessary unfolding of the graph.
- Don't convert the structures returned by `find!` into other Rust structs.
  Work with the returned tuples directly.
- `find!` should be thought of like a fundamental language primitive, like
  `if`/`match`/`for`. It is not a "database query" that returns rows to be
  converted into structs, it is a way to describe the shape of the data
  you want to see. ORM-like abstractions that try to map `find!` results
  into structs are an anti-pattern.
- Avoid reading blobs eagerly; prefer lazy reads via `ws.get::<View<...>, _>(handle)`.
  Allocate owned data only when necessary.

## Glossary

- Workspace: the repo handle that opens branches, reads blobs, commits and
  pushes.
- TribleSet: the in-memory content snapshot returned by Workspace::checkout.
- find!: the macro you use to discover entities matching a pattern (a
  descriptive type declaration).
- entity!: construct an ad‑hoc entity into a TribleSet for commit.
- LongString: zero-copy blob schema for potentially-large text.

## Closing notes

This chapter captures the pragmatic type story we use in tribles: describe
the fields you need at the place you need them, keep the full graph, and
materialize small views lazily.

## Reviewers' checklist (quick)

- Prefer find!/pattern! projections for the fields needed by the function.
- Avoid converting graph into rust structs.
- Use ws.get::<View<...>, _>(handle) for zero-copy blob reads;
  allocate only when an owned value is required.
- Match canonical tag ids via metadata::tag (KIND_* constants).
- Manager-owned repo: long-lived Repository instances should be owned by a
  session/exporter/manager; library code should accept a Workspace or
  TribleSet rather than opening piles itself.
- Use push/merge retry loops for writers; avoid holding repo locks across
  async/await points.

The sections below contain copy‑pasteable recipes for common operations.

## Idioms & code recipes

This section contains pragmatic, copy‑pasteable snippets and patterns you can
reuse. The examples intentionally use the tribles macros (attributes!, find!,
pattern!, entity!) directly — that is the intended style.

### Reviewer checklist

When reviewing code that touches tribles, look for these items:

- Does the code use find! to select only the fields it needs, rather than
  unfolding the entire graph?
- Are blob reads kept lazy (only read LongString when necessary)?
- Are push flows using the push/merge retry loop to avoid losing concurrent
  updates?
- Is the code avoiding holding the repo's Mutex across awaits and long
  blocking operations?

## Further reading and references

- See the tribles macros: attributes!, find!, pattern!, entity! in the tribles code
  for exact usage.
- Type theory: "row polymorphism", "structural typing", "width subtyping"
  if you want the formal background.
