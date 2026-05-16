# TribleSpace overlay/CAS API (blue-sky alternative)

This document proposes an alternative API and mental model for TribleSpace,
based on a small set of primitives:

- **Blobs are immutable** and addressed by handle.
- **Branches are CAS variables** (registers) that map an id -> optional handle.
- **An overlay is a tiered blob store** (reads fall through; writes go to a tier).
- **A checkout is a value** derived from a root, not a mutable object you call
  `checkout()` on.
- **Deltas are cursors**: a watcher remembers the last head and produces
  incremental updates by default.

The motivation is to address two recurring smells:

1. `Workspace` currently conflates "writer overlay/staging" with "reader view".
2. Delta operations do not fall out naturally from API usage; callers often
   compute ranges manually.

This is a blue-sky sketch: it prioritizes coherence and ergonomics over
backward compatibility.

Related:

- `triblespace_staging.md` (closure-based durability tiers / flushing)
- `triblespace_bundles.md` (bundles as roots + reachability)

## Existing model (today)

Today the repo surface is Git-shaped:

- `Repository::pull(branch)` returns a `Workspace`.
- `Workspace::checkout(selector)` reads history and unions content.
- `Workspace::commit(content, ...)` stages commit blobs in `local_blobs`.
- `Repository::try_push(&mut ws)` uploads staged blobs and CAS-updates the
  branch head.

This works, but it overloads `Workspace` with two roles:

- a mutable writer/staging object, and
- a handle for reading snapshots/deltas.

## A key refinement: reify checkouts, keep selectors pure

One of TribleSpace's strengths is that "delta as checkout" unifies history and
live updates in a single story, and delta queries can be composed with regular
constraints (`find!` + `pattern_changes!` over heterogeneous sources).

The main weakness is discoverability and the risk of re-running selector
resolution multiple times.

To address this, we can:

1. Make `checkout()` return a *reified selection* (`Checkout`) that caches the
   resolved commit set.
2. Keep selectors pure (no implicit-head semantics) and express "what changed"
   via cursors/watchers that *produce* pure selectors (plan/confirm), rather
   than embedding stateful updates into selector composition.

This keeps delta logic explicit and composable (it's still a selector), while
making the ergonomic path obvious.

### Checkout as a continuation token (type-stable `changes()`)

If we already reify checkouts, we can go a step further and let a `Checkout`
act as the continuation token for the next delta:

```rust,ignore
let mut co = layer.checkout(branch(branch_id))?;
loop {
    // "New-to-me since the previous checkout (for the same base selector)".
    co = layer.checkout(co.changes())?;
    // ... apply incremental queries ...
}
```

The tricky part in Rust is to keep the *type* stable across iterations without
resorting to type erasure.

If `Checkout<S>` stores the full selector type `S`, and `changes()` returns
something like `seen..S::base()`, then calling `checkout(co.changes())` yields a
`Checkout<Changes<S>>`, then a `Checkout<Changes<Changes<S>>>`, etc. The selector
type grows each turn.

To avoid that type churn, normalize selectors at the `Layer::checkout` boundary:

- Define an associated `Base` selector type for every selector.
- `Layer::checkout(S) -> Checkout<S::Base>` stores `S::Base` (the normalized,
  stable "dynamic end" / root selector), not the full `S`.
- `Checkout::changes()` returns a selector whose `Base` is the same stored base,
  so repeated `checkout(co.changes())` stays `Checkout<Base>`.

Sketch:

```rust,ignore
trait Selector {
    type Base: Selector + Clone;
    fn base(&self) -> Self::Base;
    fn select(&self, layer: &mut impl Layer, out: &mut CommitSet);
}

struct Checkout<S: Selector> {
    selected: CommitSet,
    base: S, // already normalized to S::Base by Layer::checkout
}

struct Changes<S: Selector> {
    since: CommitSet,
    base: S,
}

impl<S: Selector + Clone> Selector for Changes<S> {
    type Base = S; // critical: no growth
    fn base(&self) -> S { self.base.clone() }
    /* select = "reachable from base, stopping at since" */
}
```

With this, the ergonomic loop becomes possible without stateful selectors:

```
Checkout<Base> -> changes(): Changes<Base> -> checkout(..): Checkout<Base>
```

Tradeoff: this is a more complex type-level surface. If we decide that's too
heavy for the public API, we can still keep the normalization design internally
and expose a simpler cursor API (`BranchDeltaCursor`) or type-erase the selector
stored in `Checkout` (at some performance cost).

### Unifying checkout APIs

Instead of `checkout`, `checkout_metadata`, and `checkout_with_metadata`, make:

```rust,ignore
let co = layer.checkout(selector)?;
let facts = co.facts()?;        // lazy + cached
let meta = co.metadata()?;      // lazy + cached
let (facts, meta) = co.both()?; // optional single-pass
```

Internally, `Checkout` stores the resolved `CommitSet` once, so calling `facts()`
and `metadata()` does not re-run `selector.select(layer)`.

### Selector semantics and deltas (`Layer::checkout`)

If we move from a branch-bound `Workspace` to a more general `Layer::checkout()`
(where a selector may union multiple branches/roots), we lose the notion of a
single implicit "current head".

This is good: it makes selectors honest and composable. It also means we should
avoid selector forms whose meaning depends on an implicit head.

Concretely:

- `..end` (RangeTo) remains meaningful: it's just "reachable from `end`"
  (equivalent to `ancestors(end)` / `collect_reachable(end)`).
- `start..end` (Range) remains meaningful: it's "reachable from `end` until
  `start`" (our existing `collect_reachable_from_patch_until` semantics).
- `old..` (RangeFrom) and `..` (RangeFull) are **not** meaningful at the `Layer`
  level, because they require "the current head" and `Layer::checkout` may not
  have a single head.

This also makes multi-branch checkouts feel natural:

```rust,ignore
let snap = layer.checkout(union(branch(branch_a), branch(branch_b)))?;
```

Where `branch(id)` is just sugar for "resolve the branch head, then take
`..head`".

So instead of implicit-head deltas like `old_head..`, we should make *deltas*
branch-aware via an explicit cursor/watcher.

### Branch deltas via plan/confirm (reified checkout synergy)

We can keep selectors pure while still making delta behavior ergonomic by
introducing a cursor that:

1. Observes branch state exactly once (planning).
2. Produces a pure selector to compute the delta.
3. Updates its internal state only after a successful checkout (confirm).

This composes naturally with the "reified checkout" idea: checkout reification
provides a stable view of the roots/heads that were actually used, which the
cursor can then safely adopt.

```rust,ignore
let mut cursor = BranchDeltaCursor::new(branch_id);

if let Some(plan) = cursor.plan(&mut layer)? {
    let co = layer.checkout(plan.selector)?; // reified: roots resolved once
    cursor.confirm(plan, &co);               // idempotent + failure-safe
}
```

`plan.selector` can be expressed using selectors we already have today, without
any implicit head, by using the *previous selection* (the set of commits we've
already processed) as the stop set:

- Generic: `seen..new_head`
- First observation: when `seen` is empty, `seen..new_head` is equivalent to
  `..new_head` (no special-case needed)

`BranchDeltaCursor::plan()` is responsible for reading the current branch head
(`new_head`) and producing the explicit range. `confirm(...)` then unions the
reified checkout's selected commit set into `seen` so the next plan can compute
the next delta. This makes `confirm` naturally idempotent.

### Monotonic delta semantics: define away resets

If callers use deltas for incremental queries or ingestion, they typically
assume monotonicity ("I only ever add new facts"). That assumption becomes
fragile if we try to encode non-monotonic branch movement (rewinds/resets) into
the delta API.

TribleSpace's model is also explicitly monotonic/forgetful in practice:
compaction may rewrite history or make old commits unreachable without
invalidating previously observed facts.

So we can define delta semantics as:

> "Give me commits/facts reachable from the current branch head that I have not
> observed before with this cursor."

Under this definition:

- rewinds naturally yield an empty delta (no new commits),
- jumps to unrelated history yield "new-to-me" commits,
- checked fast-forward semantics are not required for correctness (topology
  changes are simply not part of the contract).

If an application truly needs "current snapshot" semantics (including
retractions), it should re-checkout the full snapshot and treat that as a
different operation from monotonic deltas.

### Why not add "post-select update" to selectors?

It's tempting to extend the selector trait with a lifecycle hook:

```rust,ignore
trait Selector {
    fn select(self, layer: &mut Layer) -> CommitSet;
    fn observe(&mut self, checkout: &Checkout) { /* update cursor */ }
}
```

But this makes selectors stateful, which breaks composition (`union`, `filter`,
etc.) and makes it unclear when updates happen if a selector is evaluated
multiple times (or fails mid-way).

The plan/confirm cursor keeps selectors pure and makes state updates explicit
and failure-safe.

## Proposed primitives

### 1) CAS variables (branches as registers)

Make the "branch" concept explicit as a CAS variable:

- Branch id = register identity
- Branch store value = `Option<BranchMetaHandle>` (or a more general root handle)

```rust,ignore
let mut layer = Layer::open(pile, signing_key);

let head: Option<BranchMetaHandle> = layer.head(branch_id)?;
layer.update(branch_id, expected, new)?;
```

This aligns with the mental model: branches are CAS variables.

### 2) Checkouts as values (reader results)

A checkout is a *value* produced by evaluation, not a mutable thing you call
`checkout()` on.

```rust,ignore
let snap: Checkout = layer.checkout(branch(branch_id))?;
let facts: TribleSet = snap.facts();
let meta: TribleSet = snap.metadata();
```

The checkout can also carry lightweight provenance:

- the selected head/root handle(s)
- the commit set or selector resolution details (optional)

### 3) Deltas as cursors/watchers

Delta behavior should be the default, not a manual optimization.

Introduce a watcher/cursor that remembers the last observed head for a branch:

```rust,ignore
let mut cursor = BranchDeltaCursor::new(branch_id);

if let Some(plan) = cursor.plan(&mut layer)? {
    let co = layer.checkout(plan.selector)?;
    cursor.confirm(plan, &co);
}
```

`plan()` can implement:

- **fast-forward**: new head is a descendant of previous head; compute changes
  incrementally (commit range or head..head).
- **fallback**: head rewound / non-ancestor; recompute once and reset the cursor.

This makes delta operations fall out naturally from usage.

### 4) Transactions as writer overlays

Writing is explicit: open a transaction on a CAS variable.

```rust,ignore
let mut tx = layer.txn(branch_id)?;

// stage blobs (policy decides tier: memory/disk/etc.)
let file = tx.put::<RawBytes, _>(bytes)?;

// create a commit (facts are persisted as a content blob in the overlay)
tx.commit(fragment, None, Some("update"))?;

// publish is: flush reachable closure + CAS update
tx.publish()?;
```

This separates:

- **evaluation** (checkout) from
- **construction** (transaction) from
- **publication** (CAS).

## New root semantics: publish branch-meta handle

In the new model, the natural "published root" is the *new branch-meta handle*.

Publishing should be closure-based:

1. Create the new branch-meta blob *in the overlay*.
2. Compute `reachable(overlay, [branch_meta_handle])`.
3. `transfer` that closure into the durable layer.
4. CAS-update the branch register to point to `branch_meta_handle`.

This avoids the "upload everything staged" trap and makes correctness depend on
reachability rather than staging discipline.

## Layering overlays (remote -> disk -> thread)

The original motivation for `Workspace` was concurrency ergonomics: a worker
needs a copyable, synchronization-free object that can be passed to a thread
without sharing a mutable repository behind a lock.

We can preserve this by making "workspace" just one kind of overlay layer, and
allowing overlays to stack:

- a remote durable layer (object store / pile / etc.)
- a disk cache overlay on the machine
- a per-thread/per-task overlay for staging commits

The key constraint is: **only the outer-most overlay is mutated by the worker**;
all lower layers are read-through and only updated via explicit publication.

## Publishing / merge-back when a Workspace becomes a Layer

One reason `Workspace` exists today is ergonomic concurrency: it's copyable,
does not require locking a shared repository, and can be passed to worker
threads. We want to preserve that.

If we generalize "workspace" into a generic `Layer`/overlay, the push story
becomes "publish this overlay into its parent layer":

1. The overlay tracks, per branch it intends to update:
   - `branch_id`
   - `expected_base_branch_meta` (for CAS)
   - `new_head` (commit handle)
   - any required branch metadata (e.g. branch name handle)
2. `publish()`:
   - constructs a new branch-meta blob in the overlay
   - computes `reachable(overlay, [branch_meta_handle])`
   - transfers that closure to the parent durable layer
   - CAS-updates the parent's branch store: `update(branch_id, expected, new)`

On CAS conflict, `publish()` can return a "conflict overlay" rooted at the
conflicting branch head/meta (like today's `try_push -> conflict_ws`). The
caller merges their staged changes into it and retries publication.

This preserves:

- lock-free worker execution (each worker gets its own overlay)
- the push/merge/retry loop
- reachability-based correctness (no implicit "upload everything staged")

## Types and traits (sketch)

```rust,ignore
pub trait Layer {
    // Branch store (CAS registers).
    fn head(&mut self, branch_id: Id) -> Result<Option<BranchMetaHandle>, Error>;
    fn update(
        &mut self,
        branch_id: Id,
        old: Option<BranchMetaHandle>,
        new: Option<BranchMetaHandle>,
    ) -> Result<PushResult<Blake3>, Error>;

    // Checkout: resolve selector once, return a reified checkout.
    fn checkout<S: CheckoutSelector>(&mut self, selector: S) -> Result<Checkout, Error>;

    // Writer overlay / staging transaction.
    fn txn(&mut self, branch_id: Id) -> Result<Txn, Error>;
}

pub struct ObservedRoot {
    branch_id: Id,
    branch_meta: Option<BranchMetaHandle>,
    head: Option<CommitHandle>,
}

pub struct Checkout {
    // A checkout may combine multiple roots/branches. For single-root checkouts
    // this is typically a 1-element list.
    roots: Vec<ObservedRoot>,
    facts: TribleSet,
    metadata: TribleSet,
}

pub struct Delta {
    old: Option<BranchMetaHandle>,
    new: Option<BranchMetaHandle>,
    // optionally: (facts_added, facts_removed?) depending on selector semantics
    facts: TribleSet,     // the incremental union to apply
    metadata: TribleSet,
}

pub struct Txn {
    branch_id: Id,
    base: Option<BranchMetaHandle>,
    overlay: OverlayStore,
    // staged commit head, branch-meta handle, etc.
}
```

An `OverlayStore` can be:

- memory tier + disk tier + durable reader
- policy-driven `put` routing

## Incremental queries as a natural layer

With a branch delta cursor producing explicit delta selectors, incremental query
APIs become natural:

```rust,ignore
let mut q = IncrementalPattern::new(pattern!([...]));
let mut cursor = BranchDeltaCursor::new(branch_id);

if let Some(plan) = cursor.plan(&mut layer)? {
    let delta = layer.checkout(plan.selector)?;
    q.apply(delta.facts());
    cursor.confirm(plan, &delta);
}
```

This avoids ad-hoc per-subsystem delta caching and centralizes "what changed".

## Compatibility / migration strategy (optional)

If we adopt this model, `Repository/Workspace` can become a façade:

- `Repository::pull` => `Layer::txn + branch cursor + checkout` under the hood
- `Workspace` => a thin wrapper around `Txn` for writers and `Checkout` for readers

But a clean break may be preferable if the model is strong enough.

## Open questions

1. What is the "root handle" type for a CAS variable?
   - keep using branch-meta handles, or
   - generalize to `Handle<UnknownBlob>` and make branch-meta a convention.
2. What does a delta mean for different selectors?
   - for `ancestors`, it's naturally additive (new commits).
   - for arbitrary ranges, it may require set subtraction or re-evaluation.
3. How much should `BranchDeltaCursor` cache?
   - last head only, or
   - also last checkout facts/metadata fingerprints, commit resolution, etc.
4. How do we represent "pre-upload"?
   - explicit `flush(roots)` for cache warming (recommended), or
   - keep an implicit behavior.

## Why this model is attractive

- Coherent primitives: blobs + CAS + overlays.
- Checkout becomes a product; transactions become explicit.
- Delta behavior is default via cursors, not a special optimization.
- Publication correctness becomes reachability-based, not staging-based.
