# Glossary

This chapter collects the core terms that appear throughout the book. Skim it
when you encounter unfamiliar terminology or need a refresher on how concepts
relate to one another in Trible Space.

### Attribute
A property that describes some aspect of an entity. Attributes occupy the
middle position in a trible and carry the `ValueSchema` (or blob-handle schema)
that interprets and validates the value. Modules mint them with the
`attributes!` macro, so they behave like detached struct fields: each attribute
remains independently typed even when many are combined to describe the same
entity, preserving its individual semantics. Provide an explicit 128-bit id in
the macro when you need a canonical column shared across crates or languages;
omit the literal to derive a deterministic id from the attribute name and value
schema (the macro calls `Attribute::from_name` for you), which is handy for
short-lived or internal attributes.

### Blob
An immutable chunk of binary data addressed by the hash of its contents. Blobs
store payloads that do not fit in the fixed 32-byte value slot—long strings,
media assets, archived `TribleSet`s, commit metadata, and other large
artifacts. Each blob is tagged with a `BlobSchema` so applications can decode it
back into native types.

### Blob Store
An abstraction that persists blobs. Implementations back local piles, in-memory
workspaces, or remote object stores while presenting a common `BlobStore`
interface that handles hashing, deduplication, and retrieval.

### Commit
A signed snapshot of repository state. Commits archive a `TribleSet` describing
the workspace contents and store metadata such as parent handles, timestamps,
authors, signatures, and optional messages. The metadata itself lives in a
`SimpleArchive` blob whose hash becomes the commit handle.

### Commit Selector
A query primitive that walks a repository’s commit graph to identify commits of
interest. Selectors power history traversals such as `parents`,
`nth_ancestor`, ranges like `a..b`, and helpers such as `history_of(entity)`.

### Entity
The first position in a trible. Entities identify the subject making a
statement and group the attributes asserted about it. They are represented by
stable identifiers so multiple facts about the same subject cohere.

In practice you pick an identifier policy:
- **Extrinsic ids** (for example `ufoid`, `fucid`, `genid`) track a conceptual
  subject across edits and versions. Use these when you intend to accumulate
  additional facts over time.
- **Intrinsic ids** (content-derived hashes) are recomputed from the entity's
  asserted fields. The `entity!` macro uses this policy when you omit the
  explicit `id @` prefix (or when you write `_ @`), so identical records unify
  naturally.

Ownership policies and schemas determine who may mint new facts for a given
identifier.

### PATCH
The **Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance**.
Each PATCH stores all six permutations of a trible set in a 256-ary trie whose
nodes use byte-oriented cuckoo hash tables and copy-on-write semantics. Shared
leaves keep permutations deduplicated, rolling hashes let set operations skip
unchanged branches, and queries only visit the segments relevant to their
bindings, mirroring the behaviour described in the deep-dive chapter.

### Pile
An append-only collection of blobs and branch records stored in a single file.
Piles act as durable backing storage for repositories, providing a
write-ahead-log style format that can be memory mapped, repaired after crashes,
and safely shared between threads.

### Repository
The durable record that ties blob storage, branch metadata, and namespaces
together. A repository coordinates synchronization, replication, and history
traversal across commits while enforcing signatures and branch ownership.

### Schema
The set of attribute declarations and codecs that document and enforce the shape
of data in Trible Space. Schemas assign language-agnostic meaning to the raw
bytes—they are not the concrete Rust types—so any implementation that
understands the schema can interpret the payloads consistently. Value schemas
map the fixed 32-byte payload of a trible to native types, while blob schemas
describe arbitrarily long payloads so tribles referencing those blobs stay
portable.

### Trible
A three-part tuple of entity, attribute, and value stored in a fixed 64-byte
layout. Tribles capture atomic facts, and query engines compose them into joins
and higher-order results.

### Trible Space
The overall storage model that organises tribles across blobs, PATCHes, and
repositories. It emphasises immutable, content-addressed data, monotonic set
semantics, and familiar repository workflows.

### Value
The third position in a trible. Values store a fixed 32-byte payload interpreted
through the attribute’s schema. They often embed identifiers for related
entities or handles referencing larger blobs.

### Workspace
A mutable working area for preparing commits. Workspaces track staged trible
sets and maintain a private blob store so large payloads can be uploaded before
publishing. Once a commit is finalised it becomes immutable like the rest of
Trible Space.
