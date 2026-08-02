# Repository Workflows

TribleSpace repositories separate immutable, content-addressed blobs from the
small amount of replicated state needed to name a line of work. That replicated
state uses the same immutable asserted-pin envelope as every other pin kind.
There is no mutable branch-head slot: publishing adds a typed branch-pin
assertion, and resolving a branch derives its current frontier from all generic
assertions known for its exact descriptor.

The main types are:

- **`Repository`** — a local authoring boundary backed by a blob store and one
  Ed25519 signing key.
- **`Workspace`** — a mutable staging area containing a branch identity, a
  current commit head, and blobs not yet uploaded to the repository.
- **`BranchIdentity`** — the exact `(author key, name handle)` pair identifying
  a branch. The name is a content-addressed `LongString` blob.
- **`BranchPinDescriptor`** — the canonical typed blob containing the branch
  kind marker and name handle. Its content handle is the generic pin handle.
- **`BranchRank`** — the branch kind's authenticated, causally monotone
  256-bit label carried through workspace provenance.
- **`PinAssertion`** — the generic signed envelope over an author, descriptor
  handle, value handle, and opaque label. For a branch, the value is a commit
  and the label is a `BranchRank`.
- **`PinAssertionStore`** — storage for one coherent, grow-only set of generic
  assertion witnesses, including kinds the current binary does not understand.

This model deliberately allows concurrent publications. If two writers publish
incomparable descendants, both assertions remain true; resolution computes the
maximal commit frontier and derives a deterministic view over it.

## Opening a repository

Construct a repository from a storage backend, a signing key, and repository-wide
commit metadata:

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;
use triblespace::core::repo::{memoryrepo::MemoryRepo, Repository};

let storage = MemoryRepo::default();
let key = SigningKey::generate(&mut OsRng);
let mut repo = Repository::new(storage, key, TribleSet::new())?;
```

`Repository::new` accepts anything convertible into a `Fragment` as commit
metadata. Embedded blobs in that fragment are stored along with the archived
metadata, so an `entity!` or `attributes!::describe()` fragment can be used
without separately managing its handles.

Repository operations are capability-gated by the backend:

- `BlobStore` is sufficient to construct a repository and create a detached
  workspace.
- `PinAssertionStore` and a reader implementing `PartialCommitDag` add
  branch resolution and pull.
- `StorageFlush` adds publication, because blobs must cross a durability
  boundary before an assertion may point at them.
- `StorageClose` adds `Repository::close` for backends needing explicit
  shutdown.

`MemoryRepo` provides all of these capabilities for tests and ephemeral work.
`Pile` provides the durable local implementation: blobs and fixed-size generic
asserted-pin records share one append-only file. Other storage compositions can
implement the same capabilities without changing the repository semantics.

## Branch identity and first publication

A branch does not exist merely because a name was chosen. Its identity is the
full pair of the repository's public key and the content handle of the name:

```text
BranchIdentity = (author verifying key, branch-name handle)
```

`BranchIdentity` is `Copy`, so callers can retain it as the stable selector used
by `resolve` and `pull`. Internally, `BranchPinDescriptor` deterministically
turns its name into a typed descriptor blob; the full 32-byte handle of that
blob, paired with the author key, is the generic asserted-pin identity. No
truncated branch id participates in selection or equality.

Create a new line of work with `create_workspace`, commit into it, and publish
the commit with `push`:

```rust,ignore
let mut ws = repo.create_workspace("main")?;
let main = *ws.identity();

ws.commit(TribleSet::new(), "initial commit")
    .expect("workspace rank has room");

let outcome = repo.push(&mut ws)?;
assert!(matches!(
    outcome,
    triblespace::core::repo::PublishOutcome::Published(_)
));

let mut reopened = repo.pull(main)?;
```

Creating the workspace stages the branch-name blob and canonical
`BranchPinDescriptor` locally. It does not write an assertion or create an
empty branch. Pushing a workspace whose head has not changed returns
`PublishOutcome::NoChange`; empty branches therefore remain unrepresentable.
The first changed push makes the staged blobs durable and adds the first
generic assertion carrying the commit value and root rank.

For an existing name owned by the repository key, callers can derive the same
descriptor without opening a workspace:

```rust,ignore
let main = repo.branch_identity("main");
let state = repo.resolve(&main)?;
let ws = repo.pull(main)?;
```

`resolve_name("main")` is shorthand for deriving the repository's own identity
and resolving it.

## Working in a workspace

A workspace keeps three things together: its immutable `BranchIdentity`, its
current optional commit head, and a private staging store layered over the blob
snapshot from which it was opened. A newly created workspace has no head;
`commit` archives a content fragment, constructs canonical signed commit
metadata, stages all resulting blobs, and advances the local head.

These operations do not mutate repository branch state. Applications may keep
several workspaces for one identity and let them advance independently. Their
only replicated effect occurs when `push` appends an assertion for a changed
head. The workspace also retains authenticated rank provenance: roots start at
zero, and each child or merge advances beyond its greatest parent rank.
`Workspace::identity`, `head`, `put`, `get`, `commit`, `checkout`, and the
explicit merge helpers all operate within this local view.

## Resolving assertions

`Repository::resolve` takes a coherent generic assertion snapshot, selects the
exact branch-pin identity, and classifies what can be established from the
commit metadata currently available locally. It
returns one of four states:

| State | Meaning | Safe next step |
| --- | --- | --- |
| `Absent` | No assertion exists for the exact identity. | Create a workspace if this is a new local branch, or ingest/fetch the missing replicated state. |
| `TipPending` | At least one surviving asserted tip's commit metadata is absent. | Fetch `missing_tips()` and resolve again. |
| `Partial` | Every surviving tip is a well-formed commit, but missing ancestry prevents the resolver from deciding all dominance relations. | Inspect `candidate_root()` only as a deterministic descriptor, fetch `missing_ancestry()`, and resolve again before checkout or authoring. |
| `Complete` | The complete, sorted, nonempty maximal antichain is known. | Inspect `tips()` or open a writable workspace with `pull`. |

Absence is distinct from incomplete replication. A generic assertion can arrive
before its branch descriptor or the commit metadata it names. A caller that
already knows the exact branch identity can still resolve it; a missing commit
produces `TipPending`, while the absent descriptor prevents enumeration by
branch kind. Readable tips can
arrive before enough ancestry to compare them, producing `Partial`. Backend
failures and malformed commit metadata remain errors rather than being
misreported as missing data.

Only `Complete` resolution licenses a writable pull. `Repository::pull`
surfaces `Absent`, `TipPending`, and `Partial` as explicit errors instead of
silently choosing a head from incomplete knowledge.

### Divergent complete frontiers

A complete frontier containing one maximal commit resolves directly to that
commit. A complete divergent frontier resolves to a canonical, flat,
authorless merge metadata blob whose parents are all maximal tips in raw-handle
order. The shape depends only on the frontier, not on assertion arrival order
or a history of pairwise merges.

That synthetic merge is a derived view, not an asserted branch-pin value. Pull stages it
as the workspace's base head. Merely pulling and pushing it unchanged returns
`NoChange`; making a new commit creates an authored descendant of the derived
merge, and pushing that descendant adds a normal generic assertion with a rank
strictly above every merged tip.

`PartialFrontier::candidate_root` uses the same flat representation over the
non-definitely-dominated candidates. It is a deterministic lower-bound
descriptor that does not discard a candidate, not an available high-level
checkout: `Repository::pull` correctly refuses a partial frontier, and walking
those candidate histories would encounter the missing ancestry. It must not be
asserted because additional ancestry could later prove a candidate dominated.

## Publishing and concurrency

`Repository::push` has one publication point and no compare-and-swap loop. For
a changed workspace it performs these steps in order:

1. Reconstruct and stage the canonical `BranchPinDescriptor` from the exact
   workspace identity.
2. Upload every staged blob.
3. Flush the storage so those blobs and the descriptor are durable.
4. Read the proposed head metadata from the durable snapshot.
5. Verify that the head has one of the canonical authored-commit or flat
   authorless-merge shapes.
6. Sign the generic `(author key, descriptor handle, commit handle, branch
   rank)` envelope and durably append it.
7. Refresh the workspace base and clear its staging area.

No fallible storage operation follows a successful assertion append. Thus a
returned `Published` assertion cannot point at a commit that was only buffered
locally. Repeating `push` without another commit returns `NoChange`, and exact
duplicate assertions are idempotent in the assertion store.

Concurrent writers do not overwrite or reject one another. Two workspaces
pulled from the same base may both publish descendants; the grow-only store
retains both assertions. A later resolution removes definitely dominated
claims and, when the remaining commits are incomparable, exposes their
canonical divergent frontier. Publication consequently chooses no losing
writer and performs no mutable-head retry.

Workspace-level `merge` and `merge_commit` remain useful when an application
deliberately combines staged work or a known commit before publication. They
operate on commit history; they are not a repair loop imposed by branch
storage.

## Local authoring boundary

One `Repository` publishes only identities owned by the signing key passed to
`Repository::new`. `branch_identity` and `create_workspace` always construct
such identities. `resolve` and `pull` reject a foreign author key before
consulting the assertion or blob stores, and `push` rejects it before uploading
workspace data or appending an assertion.

This hard boundary keeps local authoring separate from replication policy.
Accepting assertions signed by other authors is a future, explicit remote
ingest capability with its own authorization, quota, and overload rules; it is
separate from the local authoring API.

## Inspecting history

`Workspace::checkout` returns a `Checkout`, which dereferences to `TribleSet`
and also records the `CommitSet` that produced it. Passing one commit selects
that commit. Wrap a selector with `ancestors` to include its history, or use a
range to select commits reachable from the end but not the start. Missing range
endpoints mean empty (`..b`) or the current workspace head (`a..` and `..`).

```rust,ignore
let history = ws.checkout(commit_a..commit_b)?;
let full = ws.checkout(ancestors(commit_b))?;
```

The `history_of` helper filters history to commits affecting a particular
entity:

```rust,ignore
let entity_changes = ws.checkout(history_of(my_entity))?;
```

Keeping the identity separately makes repeated incremental pulls explicit:

```rust,ignore
let identity = *ws.identity();
let changed = repo.pull(identity)?.checkout(previous_commits..)?;
```

Commit selectors and incremental queries are covered in more detail in the
next chapter.

## Working with custom blobs

A workspace has a private in-memory blob store layered over the repository
reader. This lets structured facts and large payloads be staged together.
`Workspace::put` accepts anything implementing `IntoBlob` and returns a typed,
content-addressed handle suitable for embedding in an entity.

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::blob::Blob;
use triblespace::core::examples::{self, literature};
use triblespace::core::repo::{self, memoryrepo::MemoryRepo, Repository};
use triblespace::prelude::*;
use blobencodings::{LongString, SimpleArchive};

let storage = MemoryRepo::default();
let mut repo = Repository::new(
    storage,
    SigningKey::generate(&mut OsRng),
    TribleSet::new(),
)?;
let mut ws = repo.create_workspace("main")?;
let main = *ws.identity();

let quote_handle: Inline<Handle<LongString>> =
    ws.put("Fear is the mind-killer".to_owned());
let archive_handle: Inline<Handle<SimpleArchive>> =
    ws.put(&examples::dataset());

let mut change = entity! {
    literature::title: "Dune (annotated)",
    literature::quote: quote_handle.clone(),
};
change += entity! { repo::content: archive_handle.clone() };

ws.commit(change, "attach annotated dataset")
    .expect("workspace rank has room");
repo.push(&mut ws)?;

let mut reopened = repo.pull(main)?;
let restored_quote: String = reopened.get(quote_handle)?;
let restored_set: TribleSet = reopened.get(archive_handle)?;
let archive_bytes: Blob<SimpleArchive> = reopened.get(archive_handle)?;
std::fs::write("dataset.car", archive_bytes.bytes.as_ref())?;
```

`entity!` can also absorb blob-valued fields directly through `Fragment`.
Call `put` explicitly when the application needs to retain the handle for
logging, reuse, or later reads.

`Workspace::get` checks staged blobs first and then its repository snapshot.
Before publication, newly added payloads exist only in the workspace. A
successful changed `push` uploads and flushes them before appending the generic
branch-pin assertion, so handles reachable from the published commit remain
resolvable.

## Closing durable storage

When a backend implements `StorageClose`, consume the repository with
`repo.close()?` to flush and release its resources explicitly. Alternatively,
use `into_storage` when backend-specific finalization is needed:

```rust,ignore
repo.close()?;
```

Explicit close is useful for short-lived commands that should report a final
I/O failure instead of relying on `Drop`.

## Optional telemetry sink

The facade crate exposes an optional `telemetry` feature that turns `tracing`
spans into TribleSpace commits. This is useful for profiling services, import
pipelines, or long-running agents while keeping telemetry noise in a dedicated
pile.

```rust,ignore
use triblespace::telemetry::Telemetry;

let _guard = Telemetry::install_global_from_env("archive import");
```

Set `TELEMETRY_PILE` to enable the sink. You can tune batching via
`TELEMETRY_FLUSH_MS`.
