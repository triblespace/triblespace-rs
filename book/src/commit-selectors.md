# Commit Selectors

Commit selectors describe which commits to load from a workspace. They give
callers a reusable vocabulary for requests such as *"let me work with the
changes from last week"* or *"show the commits that touched this entity"*. The
selector itself only decides **which** commits participate; the data behind
those commits is materialized into a `TribleSet` by `Workspace::checkout` so the
rest of the system can query it like any other dataset.

At checkout time the `Workspace::checkout` method accepts any type implementing
the `CommitSelector` trait and returns a `TribleSet` built from the selected
commits. Selectors can be as small as a single commit handle or as expressive as
a filtered slice of history. This chapter walks through the available building
blocks, how they compose, and how they relate to Git's revision grammar.

## Range semantics

Range selectors mirror Git's two‑dot syntax. A selector of the form `a..b`
starts from `b` and walks its reachable ancestors. The walk continues until it
encounters a commit selected by `a`, at which point the descent along that
branch stops. The start boundary is **exclusive** while the end boundary is
**inclusive**: commits selected by `a` are omitted from the result, but the
commit(s) provided by `b` are included alongside any additional ancestors
reached through other branches. The shorthands behave as follows:

- `..b` is equivalent to `empty()..b` and gathers `b` plus all of its
  ancestors.
- `a..` defaults the end boundary to `HEAD`, collecting `HEAD` and its ancestors
  until the walk meets `a`.
- `..` expands to `HEAD` and every ancestor reachable from it.

Because the range semantics differ slightly from Git, you can wrap the start
boundary in `ancestors` to reproduce Git's set-difference behaviour when parity
is required: `ancestors(a)..b` matches `git log a..b`.

```rust
// Check out the entire history of the current branch
let history = ws.checkout(ancestors(ws.head()))?;

// Equivalent to `git log feature..main`
let delta = ws.checkout(ancestors(feature_tip)..main_tip)?;
```

Ranges are concise and map directly onto the ancestry walks exposed by the
repository. Combinations such as "ancestors of B that exclude commits reachable
from A" fall out naturally from existing selectors (`ancestors(A)..B`). When a
query needs additional refinement, layer selectors like `filter`, reach for
helpers such as `symmetric_diff`, or implement a small `CommitSelector` that
post-processes the resulting `CommitSet` with `union`, `intersection`, or
`difference` before handing it back to `checkout`.

Short-circuiting at the boundary avoids re-walking history that previous
selectors already covered, but it still requires visiting every reachable
commit when the start selector is empty. Long-lived queries that continuously
ingest history can avoid that re-walk by carrying forward a specific commit as
the new start boundary. If a prior run stopped at `previous_head`, the next
iteration can use the range `previous_head..new_head` to gather only the
commits introduced since the last checkout.

## Implemented selectors

`CommitSelector` is implemented for:

- `CommitHandle` – a single commit.
- `Vec<CommitHandle>` and `&[CommitHandle]` – explicit lists of commits.
- `ancestors(commit)` – a commit and all of its ancestors.
- `nth_ancestor(commit, n)` – follows the first-parent chain `n` steps.
- `parents(commit)` – direct parents of a commit.
- `symmetric_diff(a, b)` – commits reachable from either `a` or `b` but not
  both.
- Set combinators that operate on two selectors:
  - `union(left, right)` – commits returned by either selector.
  - `intersect(left, right)` – commits returned by both selectors.
  - `difference(left, right)` – commits from `left` that are not also returned
    by `right`.
- Standard ranges: `a..b`, `a..`, `..b` and `..` that stop walking once the
  start boundary is encountered.
- `filter(selector, predicate)` – retains commits for which `predicate`
  returns `true`.
- `history_of(entity)` – commits touching a specific entity (built on
  `filter`).
- `time_range(start, end)` – commits whose timestamps intersect the inclusive
  range.

The range primitives intentionally diverge from Git's subtraction semantics.
`a..b` walks the history from `b` toward the start boundary and stops as soon as
it rediscovers a commit yielded by `a`. Workspace checkouts frequently reuse an
earlier selector—such as `previous_head..new_head`—so short-circuiting at the
boundary saves re-walking the entire ancestor closure every time the selector
runs. When you need Git's behaviour you can wrap the start in
`ancestors`, trading the extra reachability work for parity with `git log`.

Because selectors already operate on `CommitSet` patches, composing new
behaviour is largely a matter of combining those sets. The existing selectors in
this chapter are implemented using the same building blocks that are available
to library users, making it straightforward to prototype project-specific
combinators without altering the `Workspace::checkout` API.

## Set combinators

`union`, `intersect`, and `difference` wrap two other selectors and forward the
results through the equivalent set operations exposed by PATCH. Reach for these
helpers when you want to combine selectors without writing a custom
`CommitSelector` implementation. Each helper accepts any selector combination
and returns the corresponding `CommitSet`:

```rust
use tribles::repo::{ancestors, difference, intersect, union};

// Everything reachable from either branch tip.
let combined = ws.checkout(union(ancestors(main), ancestors(feature)))?;

// Only the commits both branches share.
let shared = ws.checkout(intersect(ancestors(main), ancestors(feature)))?;

// Feature-only commits without the mainline history.
let feature_delta = ws.checkout(difference(ancestors(feature), ancestors(main)))?;
```

## Composing selectors

Selectors implement the `CommitSelector` trait, so they can wrap one another to
express complex logic. The pattern is to start with a broad
set—often `ancestors(ws.head())`—and then refine it. The first snippet below
layers a time window with an entity filter before handing the selector to
`Workspace::checkout`, and the follow-up demonstrates the built-in
`intersect` selector to combine two existing selectors.

```rust
use hifitime::Epoch;
use tribles::repo::{filter, history_of, intersect, time_range};

let cutoff = Epoch::from_unix_seconds(1_701_696_000.0); // 2023-12-01
let recent = filter(time_range(cutoff, Epoch::now().unwrap()), |_, payload| {
    payload.iter().any(|trible| trible.e() == &my_entity)
});

let relevant = ws.checkout(recent)?;

// Start from the result and zero in on a single entity.
let entity_history = ws.checkout(history_of(my_entity))?;

let recent_entity_commits = ws.checkout(intersect(
    time_range(cutoff, Epoch::now().unwrap()),
    history_of(my_entity),
))?;
```

## Filtering commits

The `filter` selector wraps another selector and keeps only the commits for
which a user provided closure returns `true`. The closure receives the commit
metadata and its payload, allowing inspection of authors, timestamps or the
data itself. Selectors compose, so you can further narrow a range:

```rust
use hifitime::Epoch;
use triblespace::core::repo::{filter, time_range};

let since = Epoch::from_unix_seconds(1_609_459_200.0); // 2020-12-01
let now = Epoch::now().unwrap();
let recent = ws.checkout(filter(time_range(since, now), |_, payload| {
    payload.iter().any(|t| t.e() == &my_entity)
}))?;
```

Higher level helpers can build on this primitive. For example `history_of(entity)` filters
`ancestors(HEAD)` to commits touching a specific entity:

```rust
let changes = ws.checkout(history_of(my_entity))?;
```

When debugging a complicated selector, start by checking out the wider range and
logging the commit metadata. Verifying the intermediate results catches
off-by-one errors early and helps spot situations where a filter excludes or
includes more history than expected.

## Git Comparison

The table below summarizes Git's revision grammar. Each row links back to the
official documentation. Forms that rely on reflogs or reference objects other
than commits are listed for completeness but are unlikely to be implemented.

| Git Syntax | Planned Equivalent | Reference | Status |
|-----------|-------------------|-----------|--------|
| `A` | `commit(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#_specifying_revisions) | Implemented |
| `A^`/`A^N` | `nth_parent(A, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revnegHEADv1510) | Not planned |
| `A~N` | `nth_ancestor(A, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revnegHEADmaster3) | Implemented |
| `A^@` | `parents(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revegHEAD) | Implemented |
| `A^!` | `A minus parents(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revegHEAD-1) | Unimplemented |
| `A^-N` | `A minus nth_parent(A, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-rev-negHEAD-HEAD-2) | Not planned |
| `A^0` | `commit(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revnegHEADv1510) | Implemented |
| `A^{}` | `deref_tag(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revegv0998) | Unimplemented |
| `A^{type}` | `object_of_type(A, type)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revtypeegv0998commit) | Not planned: non-commit object |
| `A^{/text}` | `search_from(A, text)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revtextegHEADfixnastybug) | Not planned: requires commit message search |
| `:/text` | `search_repo(text)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-textegfixnastybug) | Not planned: requires repository search |
| `A:path` | `blob_at(A, path)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-revpathegHEADREADMEmasterREADME) | Not planned: selects a blob not a commit |
| `:[N:]path` | `index_blob(path, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-npatheg0READMEREADME) | Not planned: selects from the index |
| `A..B` | `range(A, B)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-Thetwo-dotRangeNotation) | Implemented |
| `A...B` | `symmetric_diff(A, B)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-Thethree-dotSymmetricDifferenceNotation) | Implemented |
| `^A` | `exclude(reachable(A))` | [gitrevisions](https://git-scm.com/docs/gitrevisions#_commit_exclusions) | Unimplemented |
| `A@{upstream}` | `upstream_of(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-branchnameupstreamegmasterupstreamu) | Not planned: depends on remote config |
| `A@{push}` | `push_target_of(A)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-branchnamepushegmasterpushpush) | Not planned: depends on remote config |
| `A@{N}` | `reflog(A, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-refnamenegmaster1) | Not planned: relies on reflog history |
| `A@{<date>}` | `reflog_at(A, date)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-refnamedateegmasteryesterdayHEAD5minutesago) | Not planned: relies on reflog history |
| `@{N}` | `reflog(HEAD, N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt-neg1) | Not planned: relies on reflog history |
| `@{-N}` | `previous_checkout(N)` | [gitrevisions](https://git-scm.com/docs/gitrevisions#Documentation/gitrevisions.txt--neg-1) | Not planned: relies on reflog history |

Only a subset of Git's revision grammar will likely be supported. Selectors relying on reflog history, remote configuration, or searching commits and blobs add complexity with little benefit for workspace checkout. They are listed above for completeness but remain unplanned for now.

> Note: `range(A, B)` differs subtly from Git's two-dot syntax. It walks parents
> from `B` until a commit from `A` is encountered instead of subtracting the
> entire ancestor closure of `A`. Use `ancestors(A)..B` for Git's behaviour.

## TimeRange

Commits record when they were made via a `timestamp` attribute of type
[`NsTAIInterval`](../src/value/schemas/time.rs). When creating a commit this
interval defaults to `(now, now)` but other tools could provide a wider range
if the clock precision is uncertain. The `TimeRange` selector uses this interval
to gather commits whose timestamps fall between two `Epoch` values:

```rust
use hifitime::Epoch;
use triblespace::repo::time_range;

let since = Epoch::from_unix_seconds(1_609_459_200.0); // 2020-12-01
let now = Epoch::now().unwrap();
let tribles = ws.checkout(time_range(since, now))?;
```

This walks the history from `HEAD` and returns only those commits whose
timestamp interval intersects the inclusive range.

Internally it uses `filter(ancestors(HEAD), ..)` to check each commit's
timestamp range.

