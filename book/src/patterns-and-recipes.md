# Patterns & Recipes

This chapter collects idiomatic solutions to common problems. Each recipe is
self-contained — jump to the one that matches your situation.

## Modeling relationships

### One-to-one

An entity links to exactly one other entity via a `GenId` attribute:

```rust,ignore
attributes! {
    "..." as author: GenId;
}

let book = fucid();
let writer = fucid();
change += entity! { &book @ literature::author: &writer };
```

Query both directions: the attribute stores the forward link, and the query
engine can traverse it in reverse by binding the value and querying for the
entity:

```rust,ignore
// Forward: who wrote this book?
find!(author: Id, pattern!(&catalog, [{ book_id @ literature::author: ?author }]))

// Reverse: what did this author write?
find!(book: Id, pattern!(&catalog, [{ ?book @ literature::author: author_id }]))
```

### Many-to-many

Use a **repeated attribute** — the same entity can have multiple values for one
attribute. The `entity!` macro supports this with the `*` spread syntax:

```rust,ignore
let paper = fucid();
let tag_ml = fucid();
let tag_neuro = fucid();
change += entity! { &paper @ metadata::tag: &tag_ml };
change += entity! { &paper @ metadata::tag: &tag_neuro };
```

Or in a single entity expression:

```rust,ignore
let tags = vec![tag_ml, tag_neuro];
change += entity! { &paper @ metadata::tag*: tags.iter() };
```

Query all tags for an entity, or all entities with a tag:

```rust,ignore
// All tags on this paper
find!(tag: Id, pattern!(&catalog, [{ paper_id @ metadata::tag: ?tag }]))

// All papers with this tag
find!(paper: Id, pattern!(&catalog, [{ ?paper @ metadata::tag: tag_id }]))
```

### Hierarchies (parent/child)

Model with a `parent` attribute. Children point up to their parent:

```rust,ignore
attributes! {
    "..." as parent: GenId;
}
change += entity! { &child @ tree::parent: &parent_node };
```

For recursive traversal (all ancestors, all descendants), use `path!`:

```rust,ignore
// All ancestors of this node
find!(ancestor: Id, path!(&catalog, node_id tree::parent+ ancestor))

// All descendants (reverse: who has me as ancestor?)
find!(desc: Id, path!(&catalog, desc tree::parent+ node_id))
```

## Entity classification with tags

Use `metadata::tag` with minted `GenId` tag entities. Give tags human-readable
names via `metadata::name`:

```rust,ignore
// Mint a tag once
let kind_paper = id_hex!("A1B2C3...");  // or use trible genid for random IDs
change += entity! { &kind_paper @ metadata::name: ws.put("paper".to_owned()) };

// Tag an entity
change += entity! { &my_paper @ metadata::tag: &kind_paper };

// Find all papers
find!(paper: Id, pattern!(&catalog, [{ ?paper @ metadata::tag: kind_paper }]))
```

This is the pattern used by wiki.rs (KIND_VERSION_ID), compass.rs (KIND_GOAL_ID),
and files.rs (KIND_FILE, KIND_DIRECTORY, KIND_IMPORT). Tags are entities, not
strings — they can carry metadata, participate in queries, and be shared across
systems.

## Working with blobs

Values larger than 32 bytes live in blobs. The workspace manages their lifecycle:

```rust,ignore
// Write a blob (returns a Handle you can store in an entity)
let text_handle = ws.put("A very long string...".to_owned());
change += entity! { &doc @ article::body: text_handle };

// Read a blob back
let view: View<str> = ws.get(text_handle)?;
println!("{}", view.as_ref());
```

**When to use blobs vs values:**
- If you need to **join or filter** on the data → value (inline, 32 bytes)
- If it's **opaque content** you just store and retrieve → blob (Handle)
- Rule of thumb: names and tags are values, content and payloads are blobs

## Building entities from optional attributes

When some attributes are always present and others are conditional, build the
required part first, then conditionally extend:

```rust,ignore
let id = ufoid();
let mut change = TribleSet::new();

// Required attributes — always written together
change += entity! { &id @
    metadata::tag: &KIND_REQUEST,
    request::command: command_handle,
    request::created_at: now,
};

// Optional attributes — only if present
if let Some(cwd) = default_cwd {
    let handle = ws.put(cwd.to_owned());
    change += entity! { &id @ request::cwd: handle };
}
```

When querying, use a multi-attribute pattern for the required fields (the query
engine proves they exist), and separate queries for optional fields:

```rust,ignore
// Required: one pattern, no Option<> needed
for (id, command, created_at) in find!(
    (id: Id, cmd: TextHandle, at: Inline<NsTAIInterval>),
    pattern!(&catalog, [{
        ?id @
        metadata::tag: &KIND_REQUEST,
        request::command: ?cmd,
        request::created_at: ?at,
    }])
) {
    // Optional: separate query
    let cwd = find!(
        handle: TextHandle,
        pattern!(&catalog, [{ id @ request::cwd: ?handle }])
    ).next();
}
```

## Schema evolution

Adding a new attribute to existing entities is free — just start writing it.
Existing entities that lack the attribute are unaffected; queries that require
it simply won't match them. This is the monotonic property at work.

```rust,ignore
// V1: papers have title and author
change += entity! { &paper @ literature::title: "Dune", literature::author: &herbert };

// V2: add page_count to new papers — old papers still work fine
change += entity! { &new_paper @
    literature::title: "Foundation",
    literature::author: &asimov,
    literature::page_count: 255u64.to_inline(),
};

// Query: papers with page_count (only new ones match)
find!((paper: Id, pages: Inline<U256BE>),
    pattern!(&catalog, [{ ?paper @ literature::page_count: ?pages }]))

// Query: all papers (both old and new match)
find!(paper: Id,
    pattern!(&catalog, [{ ?paper @ literature::title: _?t }]))
```

**Removing** an attribute is not directly supported — triblesets are monotonic.
Instead, add a new attribute that supersedes the old one, and update your queries
to prefer it. The old data remains but stops being used.

## Multi-dataset queries

Queries can span multiple `TribleSet`s and even native Rust collections in a
single `find!` call using `and!`:

```rust,ignore
let local_facts = TribleSet::new();
let remote_facts = TribleSet::new();

find!((entity: Id, name: String),
    and!(
        pattern!(&local_facts, [{ ?entity @ schema::tag: &KIND_PERSON }]),
        pattern!(&remote_facts, [{ ?entity @ schema::name: ?name }])
    ))
```

The engine handles the join — it doesn't matter which dataset holds which
attributes.
