# Glossary

This chapter collects the core terms that appear throughout the book. Skim it
when you encounter unfamiliar terminology or need a refresher on how concepts
relate to one another in TribleSpace.

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

### Capability
A signed authorisation to act with a specific scope on a triblespace network.
Each capability is two `SimpleArchive` blobs: a `cap` blob carrying
`cap_subject` (the pubkey it authorises), `cap_issuer`, `cap_scope_root`, and
`metadata::expires_at`; and a `sig` blob whose `sig_signs` points at the cap
blob's handle and carries the issuer's `signed_by` + `signature_r/s`. Caps chain
off the team root (or off another cap with admin scope) and verify by walking
back to the configured `team_root`. Holders present the sig blob's handle on
connection (`OP_AUTH`); the relay enforces the verified scope on every
subsequent op. See the [Capability Auth](capability-auth.md) chapter.

### Checkout
The result of `Workspace::checkout`. A `Checkout` pairs a `TribleSet` with the
`CommitSet` that produced it. It derefs to `TribleSet` for querying and its
`AddAssign` implementation merges both facts and commit sets, making it the
natural accumulator for incremental query loops.

### Commit
A signed snapshot of repository state. Commits archive a `TribleSet` describing
the workspace contents and store metadata such as parent handles, timestamps,
authors, signatures, and optional messages. The metadata itself lives in a
`SimpleArchive` blob whose hash becomes the commit handle.

### CommitSet
A set of commit handles. `CommitSet` implements `CommitSelector` by returning
itself, which is useful for incremental deltas (e.g.,
`checkout(full.commits()..)`). Supports `union`, `intersection`, and
`difference` operations.

### Commit Selector
A query primitive that walks a repository’s commit graph to identify commits of
interest. Selectors power history traversals such as `parents`,
`nth_ancestors`, ranges like `a..b`, and helpers such as `history_of(entity)`.

### Constraint
The trait that every query operator implements. A constraint exposes six methods
— `variables`, `estimate`, `propose`, `confirm`, `satisfied`, and `influence`
— that let the Atreides join engine navigate the search space without a
separate planner. Custom data sources and application predicates participate in
queries by implementing this trait.

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

### Fragment
A bundle of tribles and exported IDs returned by the `entity!` macro and import
pipelines. Fragments compose via `+=` to build larger datasets. Use
`Fragment::root()` to extract derived IDs, `Fragment::empty()` to start
accumulation, and spread (`*`) to pass child fragments into parent entities,
giving Merkle trees for free.

### PATCH
The **Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance**.
A single PATCH stores one ordering of a trible set in a 256-ary trie whose
nodes use byte-oriented cuckoo hash tables and copy-on-write semantics. A
`TribleSet` maintains six PATCH instances — one per permutation of entity,
attribute, and value. Shared leaves keep permutations deduplicated, rolling
hashes let set operations skip unchanged branches, and queries only visit the
segments relevant to their bindings, further described in
[the deep-dive chapter](deep-dive/patch.md).

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
of data in TribleSpace. Schemas assign language-agnostic meaning to the raw
bytes—they are not the concrete Rust types—so any implementation that
understands the schema can interpret the payloads consistently. Value schemas
map the fixed 32-byte payload of a trible to native types, while blob schemas
describe arbitrarily long payloads so tribles referencing those blobs stay
portable.

### Scope
The set of permissions a [Capability](#capability) grants. Encoded as tribles
hung off the cap's `cap_scope_root` entity: one or more `metadata::tag: PERM_*`
triples (`PERM_READ`, `PERM_WRITE`, `PERM_ADMIN`) optionally combined with
`scope_branch: <branch_id>` triples that restrict the permission to specific
branches. An empty branch-restriction set means "every branch within the
permission set." Sub-capabilities issued via delegation must have a scope that
is a subset of the parent's; the verifier enforces this via `scope_subsumes`
during chain walk.

### Team Root
The single immutable keypair that anchors a triblespace network's
[capability](#capability) chain. Generated once at team creation, used to sign
exactly one capability (the founder's), and then archived offline — the team
root never operates online. Like a CA: bootstrapping authority, not runtime
authority. The relay hard-codes the team root pubkey via
`PeerConfig.team_root` and rejects any cap chain that doesn't terminate at it.

### Trible
A three-part tuple of entity, attribute, and value stored in a fixed 64-byte
layout. Tribles capture atomic facts, and query engines compose them into joins
and higher-order results.

### TribleSpace
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
TribleSpace.
