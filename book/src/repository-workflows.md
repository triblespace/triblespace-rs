# Repository Workflows

Working with a Tribles repository feels familiar to Git users, but the types
make data ownership and lifecycle explicit. Keep the following vocabulary in
mind when exploring the API:

* **Repository** – top-level object that tracks history through `BlobStore`
  and `BranchStore` implementations.
* **Workspace** – mutable view of a branch, similar to Git's working directory
  and index combined. Workspaces buffer commits and custom blobs until you push
  them back to the repository.
* **BlobStore** – storage backend for commits and payload blobs.
* **BranchStore** – records branch metadata and head pointers.

Both stores can be in memory, on disk or backed by a remote service. The
examples in `examples/repo.rs` and `examples/workspace.rs` showcase these APIs
and are a great place to start if you are comfortable with Git but new to
Tribles.

## Opening a repository

Repositories are constructed from any storage that implements the appropriate
traits. The choice largely depends on your deployment scenario:

1. Pick or compose a storage backend (see [Storage Backends and
   Composition](#storage-backends-and-composition)).
2. Create a signing key for the identity that will author commits.
3. Call `Repository::new(storage, signing_key)` to obtain a handle.

Most applications perform the above steps once during start-up and then reuse
the resulting `Repository`. If initialization may fail (for example when opening
an on-disk pile), bubble the error to the caller so the process can retry or
surface a helpful message to operators.

## Storage Backends and Composition

`Repository` accepts any storage that implements both the `BlobStore` and
`BranchStore` traits, so you can combine backends to fit your deployment. The
crate ships with a few ready-made options:

- [`MemoryRepo`](../src/repo/memoryrepo.rs) stores everything in memory and is
  ideal for tests or short-lived tooling where persistence is optional.
- [`Pile`](../src/repo/pile.rs) persists blobs and branch metadata in a single
  append-only file. It is the default choice for durable local repositories and
  integrates with the pile tooling described in [Pile Format](pile-format.md).
- [`ObjectStoreRemote`](../src/repo/objectstore.rs) connects to
  [`object_store`](https://docs.rs/object_store/latest/object_store/) endpoints
  (S3, local filesystems, etc.). It keeps all repository data in the remote
  service and is useful when you want a shared blob store without running a
  dedicated server.
- [`HybridStore`](../src/repo/hybridstore.rs) lets you split responsibilities,
  e.g. storing blobs on disk while keeping branch heads in memory or another
  backend. Any combination that satisfies the trait bounds works.

Backends that need explicit shutdown can implement `StorageClose`. When the
repository type exposes that trait bound you can call `repo.close()?` to flush
and release resources instead of relying on `Drop` to run at an unknown time.
This is especially handy for automation where the process may terminate soon
after completing a task.

```rust,ignore
use triblespace::core::repo::hybridstore::HybridStore;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::objectstore::ObjectStoreRemote;
use triblespace::core::repo::Repository;
use triblespace::core::value::schemas::hash::Blake3;
use url::Url;

let blob_remote: ObjectStoreRemote<Blake3> =
    ObjectStoreRemote::with_url(&Url::parse("s3://bucket/prefix")?)?;
let branch_store = MemoryRepo::default();
let storage = HybridStore::new(blob_remote, branch_store);
let mut repo = Repository::new(storage, signing_key);

// Work with repo as usual …
// repo.close()?; // if the underlying storage supports StorageClose
```

## Branching

A branch records a line of history and carries the metadata that identifies who
controls updates to that history. Creating one writes initial metadata to the
underlying store and returns an [`ExclusiveId`](../src/id.rs) guarding the
branch head. Dereference that ID when you need a plain [`Id`](../src/id.rs) for
queries or workspace operations.

Typical steps for working on a branch look like:

1. Create a repository backed by blob and branch stores via `Repository::new`.
2. Initialize or look up a branch ID with helpers like
   `Repository::create_branch`. When interacting with an existing branch call
   `Repository::pull` directly.
3. Commit changes in the workspace using `Workspace::commit`.
4. Push the workspace with `Repository::push` (or handle conflicts manually via
   `Repository::try_push`) to publish those commits.

The example below demonstrates bootstrapping a new branch and opening multiple
workspaces on it. Each workspace holds its own staging area, so remember to push
before sharing work or starting another task.


```rust
let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
let branch_id = repo.create_branch("main", None).expect("create branch");

let mut ws = repo.pull(*branch_id).expect("pull branch");
let mut ws2 = repo.pull(ws.branch_id()).expect("open branch");
```

After committing changes you can push the workspace back. `push` will retry on
contention and attempt to merge, while `try_push` performs a single attempt and
returns `Ok(Some(conflict_ws))` when the branch head moved. Choose the latter
when you need explicit conflict handling:

```rust
ws.commit(change, Some("initial commit"));
repo.push(&mut ws)?;
```

### PATCH under the hood

Set operations such as merges, diffs, and intersections rely on PATCH (the
Persistent Adaptive Trie with Cuckoo-compression and Hash-maintenance). PATCH
stores each branch node in a compact cuckoo table, grows only when needed, and
keeps a 128‑bit fingerprint per subtree. Those hashes allow union and
intersection to short-circuit identical subtrees while copy-on-write updates
retain persistence so existing readers see a stable view. Understanding that
structure helps explain why repository workflows remain fast even as histories
grow.

### Managing signing identities

The key passed to `Repository::new` becomes the default signing identity for
branch metadata and commits. Collaborative projects often need to switch
between multiple authors or assign a dedicated key to automation. You can
adjust the active identity in three ways:

* `Repository::set_signing_key` replaces the repository's default key. Subsequent
  calls to helpers such as `Repository::create_branch` or `Repository::pull` use the new
  key for any commits created from those workspaces.
* `Repository::create_branch_with_key` signs a branch's metadata with an explicit
  key, allowing each branch to advertise the author responsible for updating it.
* `Repository::pull_with_key` opens a workspace that will sign its future commits
  with the provided key, regardless of the repository default.

The snippet below demonstrates giving an automation bot its own identity while
letting a human collaborator keep theirs:

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::repo::Repository;

let alice = SigningKey::generate(&mut OsRng);
let automation = SigningKey::generate(&mut OsRng);

// Assume `pile` was opened earlier, e.g. via `Pile::open` as shown in previous sections.
let mut repo = Repository::new(pile, alice.clone());

// Create a dedicated branch for the automation pipeline using its key.
let automation_branch = repo
    .create_branch_with_key("automation", None, automation.clone())?
    .release();

// Point automation jobs at their dedicated identity by default.
repo.set_signing_key(automation.clone());
let mut bot_ws = repo.pull(automation_branch)?;

// Humans can opt into their own signing identity even while automation remains
// the repository default.
let mut human_ws = repo.pull_with_key(automation_branch, alice.clone())?;
```

`human_ws` and `bot_ws` now operate on the same branch but will sign their
commits with different keys. This pattern is useful when rotating credentials or
running scheduled jobs under a service identity while preserving authorship in
the history. You can swap identities at any time; existing workspaces keep the
key they were created with until you explicitly call
`Workspace::set_signing_key`.

## Inspecting History

You can explore previous commits using `Workspace::checkout` which returns a
`TribleSet` with the union of the specified commit contents. Passing a single
commit returns just that commit. To include its history you can use the
`ancestors` helper. Commit ranges are supported for convenience. The expression
`a..b` yields every commit reachable from `b` that is not reachable from `a`,
treating missing endpoints as empty (`..b`) or the current `HEAD` (`a..` and
`..`). These selectors compose with filters, so you can slice history to only
the entities you care about.

```rust
let history = ws.checkout(commit_a..commit_b)?;
let full = ws.checkout(ancestors(commit_b))?;
```

The [`history_of`](../src/repo.rs) helper builds on the `filter` selector to
retrieve only the commits affecting a specific entity. Commit selectors are
covered in more detail in the next chapter:

```rust
let entity_changes = ws.checkout(history_of(my_entity))?;
```

## Working with Custom Blobs

Workspaces keep a private blob store that mirrors the repository's backing
store. This makes it easy to stage large payloads alongside the trible sets you
plan to commit. The [`Workspace::put`](../src/repo.rs) helper stores any type
implementing [`ToBlob`](crate::blob::ToBlob) and returns a typed handle you can
embed like any other value. Handles are `Copy`, so you can commit them and reuse
them to fetch the blob later.

The example below stages a quote and an archived `TribleSet`, commits both, then
retrieves them again with strongly typed and raw views. In practice you might
use this pattern to attach schema migrations, binary artifacts, or other payloads
that should travel with the commit:

```rust
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::blob::Blob;
use triblespace::examples::{self, literature};
use triblespace::prelude::*;
use triblespace::repo::{self, memoryrepo::MemoryRepo, Repository};
use blobschemas::{LongString, SimpleArchive};

let storage = MemoryRepo::default();
let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
let branch_id = repo.create_branch("main", None).expect("create branch");
let mut ws = repo.pull(*branch_id).expect("pull branch");

// Stage rich payloads before creating a commit.
let quote_handle = ws.put("Fear is the mind-killer".to_owned());
let archive_handle = ws.put(&examples::dataset());

// Embed the handles inside the change set that will be committed.
let mut change = triblespace::entity! {
    literature::title: "Dune (annotated)",
    literature::quote: quote_handle.clone(),
};
change += triblespace::entity! { repo::content: archive_handle.clone() };

ws.commit(change, Some("Attach annotated dataset"));
// Single-attempt push. Use `push` to let the repository merge and retry automatically.
repo.try_push(&mut ws).expect("try_push");

// Fetch the staged blobs back with the desired representation.
let restored_quote: String = ws
    .get(quote_handle)
    .expect("load quote");
let restored_set: TribleSet = ws
    .get(archive_handle)
    .expect("load dataset");
let archive_bytes: Blob<SimpleArchive> = ws
    .get(archive_handle)
    .expect("load raw blob");
std::fs::write("dataset.car", archive_bytes.bytes.as_ref()).expect("persist archive");
```

Rust infers the blob schema for both `put` and `get` from the handles and the
assignment context, so the calls stay concise without explicit turbofish
annotations.

Blobs staged this way stay local to the workspace until you push the commit.
`Workspace::get` searches the workspace-local store first and falls back to the
repository if necessary, so the handles remain valid after you publish the
commit. This round trip lets you persist logs, archives, or other auxiliary
files next to your structured data without inventing a separate storage
channel.

## Merging and Conflict Handling

When pushing a workspace another client might have already updated the branch.
There are two ways to handle this:

- `Repository::try_push` — a single-attempt push that uploads local blobs and
  attempts a CAS update once. If the branch advanced concurrently it returns
  `Ok(Some(conflict_ws))` so callers can merge and retry explicitly:

```rust
ws.commit(content, Some("codex-turn"));
let mut current_ws = ws;
while let Some(mut incoming) = repo.try_push(&mut current_ws)? {
    // Merge the local staged changes into the incoming workspace and retry.
    incoming.merge(&mut current_ws)?;
    current_ws = incoming;
}
```

- `Repository::push` — a convenience wrapper that performs the merge-and-retry
  loop for you. Call this when you prefer the repository to handle conflicts
  automatically; it either succeeds (returns `Ok(())`) or returns an error.

```rust
ws.commit(content, Some("codex-turn"));
repo.push(&mut ws)?; // will internally merge and retry until success
```

> **Troubleshooting:** `Workspace::merge` succeeds only when both workspaces
> share a blob store. Merging a workspace pulled from a different pile or
> remote returns `MergeError::DifferentRepos`. Decide which repository will own
> the combined history, transfer the other branch's reachable blobs into it with
> `repo::transfer(reachable(...))`, create a branch for that imported head, and
> merge locally once both workspaces target the same store.

After a successful push the branch may have advanced further than the head
supplied, because the repository refreshes its view after releasing the lock.
An error indicating a corrupted pile does not necessarily mean the push failed;
the update might have been written before the corruption occurred.

This snippet is taken from [`examples/workspace.rs`](../examples/workspace.rs).
The [`examples/repo.rs`](../examples/repo.rs) example demonstrates the same
pattern with two separate workspaces. The returned `Workspace` already contains
the remote commits, so after merging your changes you push that new workspace to
continue.

## Typical CLI Usage

There is a small command line front-end in the
[`trible`](https://github.com/triblespace/trible) repository. It exposes push
and merge operations over simple commands and follows the same API presented in
the examples. The tool is currently experimental and may lag behind the library,
but it demonstrates how repository operations map onto a CLI.

## Diagram

A simplified view of the push/merge cycle:

```text

        ┌───────────┐         pull          ┌───────────┐
        | local ws  |◀───────────────────── |   repo    |
        └─────┬─────┘                       └───────────┘
              │
              │ commit
              │                                                                      
              ▼                                   
        ┌───────────┐         push          ┌───────────┐
        │  local ws │ ─────────────────────▶│   repo    │
        └─────┬─────┘                       └─────┬─────┘
              │                                   │
              │ merge                             │ conflict?
              └──────▶┌─────────────┐◀────────────┘
                      │ conflict ws │       
                      └───────┬─────┘
                              │             ┌───────────┐
                              └────────────▶|   repo    │
                                     push   └───────────┘
   
```

Each push either succeeds or returns a workspace containing the other changes.
Merging incorporates your commits and the process repeats until no conflicts
remain.

### Troubleshooting push, branch, and pull failures

`Repository::push`, `Repository::create_branch`, and `Repository::pull` surface
errors from the underlying blob and branch stores. These APIs intentionally do
not hide storage issues, because diagnosing an I/O failure or a corrupt commit
usually requires operator intervention. The table below lists the error variants
along with common causes and remediation steps.

| API | Error variant | Likely causes and guidance |
| --- | --- | --- |
| `Repository::push` | `PushError::StorageBranches` | Enumerating branch metadata in the backing store failed. Check connectivity and credentials for the branch store (for example, the object-store bucket, filesystem directory, or HTTP endpoint). |
| `Repository::push` | `PushError::StorageReader` | Creating a blob reader failed before any transfer started. The blob store may be offline, misconfigured, or returning permission errors. |
| `Repository::push` | `PushError::StorageGet` | Fetching existing commit metadata failed. The underlying store returned an error or the metadata blob could not be decoded, which often signals corruption or truncated uploads. Inspect the referenced blob in the store to confirm it exists and is readable. |
| `Repository::push` | `PushError::StoragePut` | Uploading new content or metadata blobs failed. Look for transient network failures, insufficient space, or rejected writes in the blob store logs. Retrying after fixing the storage issue will re-send the missing blobs. |
| `Repository::push` | `PushError::BranchUpdate` | Updating the branch head failed. Many backends implement optimistic compare-and-swap semantics; stale heads or concurrent writers therefore surface here as update errors. Refresh the workspace and retry after resolving any store-side errors. |
| `Repository::push` | `PushError::BadBranchMetadata` | The branch metadata could not be parsed. Inspect the stored metadata blobs for corruption or manual edits and repair them before retrying the push. |
| Branch creation APIs | `BranchError::StorageReader` | Creating a blob reader failed. Treat this like `PushError::StorageReader`: verify the blob store connectivity and credentials. |
| Branch creation APIs | `BranchError::StorageGet` | Reading branch metadata during initialization failed. Check for corrupted metadata blobs or connectivity problems. |
| Branch creation APIs | `BranchError::StoragePut` | Persisting branch metadata failed. Inspect store logs for rejected writes or quota issues. |
| Branch creation APIs | `BranchError::BranchHead` | Retrieving the current head of the branch failed. This usually points to an unavailable branch store or inconsistent metadata. |
| Branch creation APIs | `BranchError::BranchUpdate` | Updating the branch entry failed. Resolve branch-store errors and ensure no other writers are racing the update before retrying. |
| Branch creation APIs | `BranchError::AlreadyExists` | A branch with the requested name already exists. Choose a different name or delete the existing branch before recreating it. |
| Branch creation APIs | `BranchError::BranchNotFound` | The specified base branch does not exist. Verify the branch identifier and that the base branch has not been deleted. |
| `Repository::pull` | `PullError::BranchNotFound` | The branch is missing from the repository. Check the branch name/ID and confirm that it has not been removed. |
| `Repository::pull` | `PullError::BranchStorage` | Accessing the branch store failed. This mirrors `BranchError::BranchHead` and usually indicates an unavailable or misconfigured backend. |
| `Repository::pull` | `PullError::BlobReader` | Creating a blob reader failed before commits could be fetched. Ensure the blob store is reachable and that the credentials grant read access. |
| `Repository::pull` | `PullError::BlobStorage` | Reading commit or metadata blobs failed. Investigate missing objects, network failures, or permission problems in the blob store. |
| `Repository::pull` | `PullError::BadBranchMetadata` | The branch metadata is malformed. Inspect and repair the stored metadata before retrying the pull. |

## Remote Stores

Remote deployments use the [`ObjectStoreRemote`](../src/repo/objectstore.rs)
backend to speak to any service supported by the
[`object_store`](https://docs.rs/object_store/latest/object_store/) crate (S3,
Google Cloud Storage, Azure Blob Storage, HTTP-backed stores, the local
filesystem, and the in-memory `memory:///` adapter). `ObjectStoreRemote`
implements both `BlobStore` and `BranchStore`, so the rest of the repository API
continues to work unchanged – the only difference is the URL you pass to
`with_url`.

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;
use triblespace::core::repo::objectstore::ObjectStoreRemote;
use triblespace::core::repo::Repository;
use triblespace::core::value::schemas::hash::Blake3;
use url::Url;

fn open_remote_repo(raw_url: &str) -> anyhow::Result<()> {
    let url = Url::parse(raw_url)?;
    let storage = ObjectStoreRemote::<Blake3>::with_url(&url)?;
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));

    let branch_id = repo.create_branch("main", None)?;
    let mut ws = repo.pull(*branch_id)?;
    ws.commit(TribleSet::new(), Some("initial commit"));

    while let Some(mut incoming) = repo.try_push(&mut ws)? {
        incoming.merge(&mut ws)?;
        ws = incoming;
    }

    Ok(())
}
```

`ObjectStoreRemote` writes directly through to the backing service. It
implements `StorageClose`, but the implementation is a no-op, so dropping the
repository handle is usually sufficient. Call `repo.close()` if you prefer an
explicit shutdown step.

Credential configuration follows the `object_store` backend you select. For
example, S3 endpoints consume AWS access keys or IAM roles, while
`memory:///foo` provides a purely in-memory store for local testing. Once the
URL resolves, repositories backed by piles and remote stores share the same
workflow APIs.

## Attaching a Foreign History (merge-import)

Sometimes you want to graft an existing branch from another pile into your
current repository without rewriting its commits. Tribles supports a
conservative, schema‑agnostic import followed by a single merge commit:

1. Copy all reachable blobs from the source branch head into the target pile
   by streaming the `reachable` walker into `repo::transfer`. The traversal
   scans every 32‑byte aligned chunk and enqueues any candidate that
   dereferences in the source.
2. Create a single merge commit that has two parents: your current branch head
   and the imported head. No content is attached to the merge; it simply ties
   the DAGs together.

This yields a faithful attachment of the foreign history — commits and their
content are copied verbatim, and a one‑off merge connects both histories.

The `trible` CLI exposes this as:

```sh
trible branch merge-import \
  --from-pile /path/to/src.pile --from-name source-branch \
  --to-pile   /path/to/dst.pile --to-name   self
```

Internally this uses the `reachable` walker in combination with
`repo::transfer` plus `Workspace::merge_commit`. Because the traversal scans
aligned 32‑byte chunks, it is forward‑compatible with new formats as long as
embedded handles remain 32‑aligned.

> **Sidebar — Choosing a copy routine**
> - `repo::transfer` pairs the reachability walker (or any other iterator you
>   provide) with targeted copies, returning `(old_handle, new_handle)` pairs
>   for the supplied handles. Feed it the `reachable` iterator when you only
>   want live blobs, the output of
>   [`potential_handles`](https://docs.rs/triblespace/latest/triblespace/repo/fn.potential_handles.html)
>   when scanning metadata, or a collected list from
>   `BlobStoreList::blobs()` when duplicating an entire store.
> - `MemoryBlobStore::keep` (and other `BlobStoreKeep` implementations) retain
>   whichever handles you stream to them, making it easy to drop unreachable
>   blobs once you've walked your roots.
>
> Reachable copy keeps imports minimal; the transfer helper lets you rewrite
> specific handles while duplicating data into another store.

### Programmatic example (Rust)

The same flow can be used directly from Rust when you have two piles on disk and
want to attach the history of one branch to another:

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;
use triblespace::core::repo::{self, pile::Pile, Repository};
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::core::value::schemas::hash::Handle;

fn merge_import_example(
    src_path: &std::path::Path,
    src_branch_id: triblespace::id::Id,
    dst_path: &std::path::Path,
    dst_branch_id: triblespace::id::Id,
) -> anyhow::Result<()> {
    // 1) Open source (read) and destination (write) piles
    let mut src = Pile::open(src_path)?;
    src.restore()?;
    let mut dst = Pile::open(dst_path)?;
    dst.restore()?;

    // 2) Resolve source head commit handle
    let src_head: Value<Handle<Blake3, blobschemas::SimpleArchive>> =
        src.head(src_branch_id)?.ok_or_else(|| anyhow::anyhow!("source head not found"))?;

    // 3) Conservatively copy all reachable blobs from source → destination
    let reader = src.reader()?;
    let mapping: Vec<_> = repo::transfer(
        &reader,
        &mut dst,
        repo::reachable(&reader, [src_head.transmute()]),
    )
    .collect::<Result<_, _>>()?;
    eprintln!("copied {} reachable blobs", mapping.len());

    // 4) Attach via a single merge commit in the destination branch
    let mut repo = Repository::new(dst, SigningKey::generate(&mut OsRng));
    let mut ws = repo.pull(dst_branch_id)?;
    ws.merge_commit(src_head)?; // parents = { current HEAD, src_head }

    // 5) Push with standard conflict resolution
    while let Some(mut incoming) = repo.try_push(&mut ws)? {
        incoming.merge(&mut ws)?;
        ws = incoming;
    }
    Ok(())
}
```

## Deepen this topic

- The [Architecture](architecture.md) chapter unpacks how blob stores, branch
  stores, and repositories coordinate to make these workflows durable.
- [PATCH under the hood](#patch-under-the-hood) explains how commit payloads are
  represented and combined during merges and selectors.
- [Pile Format](pile-format.md) and [Pile Blob Metadata](pile-blob-metadata.md)
  detail the on-disk layout used by the repository workflows above.
