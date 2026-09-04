# Collection Workflows

TribleSpace publishes data into self-describing grow-only collections. A
collection has no mutable head and no privileged linear history: independent
signed commits coexist, replicas combine records by set union, and stored
merge or derivation equations preserve reusable physical work.

## Vocabulary

- **`Fragment`** — facts, descriptive metafacts, exported IDs, and referenced
  blob attachments produced as one composable value.
- **`BlobStore`** — immutable content-addressed bytes.
- **`CollectionStore`** — a grow-only set of native `COMMIT`, `MERGE`, and
  `DERIVE` records.
- **Collection descriptor** — a canonical `SimpleArchive` which describes a
  collection's anchor, member encoding, and independent READ and WRITE
  admission policies. A derived descriptor
  additionally links one concrete mapping entity carrying its algorithm and
  concrete parameters. The descriptor's content handle is the
  `CollectionHandle`.
- **`Collection<E>`** — a cheap descriptor handle whose
  `CollectionEncoding` type `E` owns the canonical member bytes and join.
  Constructing it validates that the runtime descriptor names `E`.
- **`Cover<E>`** — one typed collection identity plus a PATCH of distinct
  `Handle<E>` members selected for one read or derivation.
  Signatures, authors, and metadata remain queryable provenance, but are not
  coordinates of the value. Checked union, intersection, difference, and
  subset operations reject covers from another collection.
  `collection.cover(members)` names such a coordinate without store access,
  which lets a durable manifest preserve an exact cover; it does not by itself
  admit, evidence, or make those members resident.
- **`Support`** — exactly `Cover<SimpleArchive>`: the distinct admitted
  `COMMIT.data` handles at the foundational fact collection. It is the
  denotational coordinate shared by every representation. `MERGE` and
  `DERIVE` replace physical work without changing it.
- **`TryFromCover<E>`** — the encoding-specific reconstruction hook used by a
  collection snapshot. A view may join eagerly or retain mmap-backed shards
  and query their union lazily.
- **`CollectionSnapshot<R, E>`** — one immutable store snapshot together with
  the foundational `Support` and the resident `Cover<E>` which realizes
  exactly that support in this observation. It reconstructs a caller-chosen
  logical value later with `view`.
- **WANT** — an orthogonal local request for content or existing computation;
  it is neither collection membership nor authority.

`MemoryRepo`, `Pile`, and the storage composition wrappers implement both the
blob and native collection surfaces. A collection is its descriptor handle;
the store remains the sole owner of I/O, durability, and lifetime.

An existing descriptor is opened through the same frozen read boundary used
for later observation:

```rust,ignore
let snapshot = storage.snapshot()?;
let models = Collection::<SimpleArchive>::open(&snapshot, collection_handle)?;
```

`open` fetches and validates the canonical descriptor and checks that its
member encoding is `SimpleArchive`. It never registers, rewrites, or otherwise
mutates the store.

## Publish a root collection

Register the descriptor once, then pass its returned handle to store
operations:

```rust,ignore
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::{
    blob::encodings::simplearchive::SimpleArchive,
    collection::{grant_collection_write, AdmissionPolicy, CollectionPolicy},
};
use triblespace::prelude::*;

let team_key = SigningKey::generate(&mut OsRng);
let writer = SigningKey::generate(&mut OsRng);
let team = team_key.verifying_key();
let writer_subject = writer.verifying_key();
let mut storage = MemoryRepo::default();
let models = storage.collection(
    "models",
    CollectionPolicy::new(
        AdmissionPolicy::direct(team),
        AdmissionPolicy::direct(team),
    ),
)?;
let _proof = grant_collection_write(
    &mut storage,
    models.handle(),
    &team_key,
    writer_subject,
)?;

let commit = storage.commit(
    models,
    &writer,
    entity! { metadata::name: "first-model" },
)?;
let snapshot = storage.snapshot()?;
let instant = triblespace::core::clock::epoch_now();
let cover = models.admitted_at(&snapshot, instant)?;
assert!(cover.contains(Handle::<SimpleArchive>::from_hash(commit.data())));
storage.flush()?;
```

Local publication deliberately performs no authorization check: the local
store is a grow-only record ledger, not an access-control boundary. Observation
loads the independent policies from the descriptor. A policy root is admitted
directly; every other author needs enough resident proof paths for exact
`ACTION_WRITE` on this descriptor. Each operation observes the clock once and
verifies every matching proof. Invalid, expired, or irrelevant
candidate evidence grants nothing; inability to enumerate the proof store
remains an error.

READ and WRITE are explicit because both participate in collection identity.
Either may be `Open` or a canonical quorum over capability roots, with
one semantic threshold. Derived collections state their own policies rather
than inheriting ambient authority from a source or a network-wide team scope.
Whether a subject may delegate a share onward is carried by the signed mode of
that independently rooted proof path, not by a second policy threshold.

### What publication writes

One `store.commit(collection, signer, fragment)` performs these semantic steps:

1. consume the fragment once into attachments, facts, and metafacts;
2. store the fragment's attachments;
3. encode and store the facts as the canonical `SimpleArchive` member;
4. encode and store metafacts as the mandatory canonical metadata
   `SimpleArchive`;
5. insert a signed `COMMIT` naming the already typed collection, data, and
   metadata handles.

Dependencies precede the record which gives them authority. Publication does
not flush implicitly. Call `flush()` at the application's chosen durability
boundary or explicitly close the backend. Repeating the same fragment with the
same signer produces the same exact signed record and is a set no-op; distinct
attestations coexist.

`COMMIT` is deliberately a source operation over authored `Fragment` values.
Other collection encodings enter the lattice through reproducible `DERIVE` and
`MERGE` records rather than alternative signed leaf formats.

Importers which must validate additional artifacts before making the source
commit visible use the same path with an explicit pause before step 5:

```rust,ignore
let prepared = PreparedCollectionCommit::from_fragment(candidate);
let mut staged = prepared.stage_for(&mut storage, models, &signer)?;

// Dependencies are resident, but COMMIT is still withheld. Any validation or
// reproducible DERIVE/MERGE publication can use this exact store now.
validate_candidate(staged.store_mut())?;

let commit = staged.finalize()?; // the sole signed COMMIT insertion
```

Preparation is store-free and dropping either a prepared or staged value never
publishes a commit. `stage_for` accepts `Collection<SimpleArchive>`, not a raw
handle or a reconstructed descriptor fragment.

## The native algebra

The collection descriptor is the only collection-control structure represented
as a trible archive. The algebra records are fixed-width native records:

```text
COMMIT(collection, data, metadata, author, signature)  // 192 bytes
MERGE(collection, low, high, result)                   // 128 bytes
DERIVE(target, input, output)                          //  96 bytes
```

`COMMIT` is a signed exogenous assertion: no machine can recompute whether an
author intended to publish a member. `MERGE` is an exact join equation within
one collection. `DERIVE` is one observation of the mapping linked by its
target descriptor; that descriptor already names its source, mapping
algorithm, and concrete mapping parameters.

Merge inputs are canonically ordered and the exact native record is the set
element. `CollectionStore::insert` therefore implements set insertion rather
than an update. Concatenating stores unions evidence. Collection records have
no synthetic entity identity; a backend may compute a full-width fingerprint
as a nonsemantic lookup key, but support, provenance, authorization, and
deduplication are defined over the exact records and payload handles.

Unsigned equations are materialized computation, not authority. Publishing a
`MERGE` or `DERIVE` records work which has already been performed; warm
resolution follows that equation without executing the join or mapping again.
Equation trust belongs at the store/synchronization boundary. Blob residency
is independent: an absent result is a cache miss and cannot suppress an
available explicit cover member.

Local publication remains unconditional. A publisher which needs to predict
whether an authority-aware observation will admit a signer can freeze a store
snapshot, sample one instant, and call
`collection.writer_is_admitted_at(&snapshot, signer, instant)`: it checks
the descriptor WRITE policy and resident exact authorization evidence without scanning
collection commits or publishing anything.

## Known-prefix snapshots and covers

`store.snapshot()` freezes one immutable observation containing blob bytes,
collection records, and capability proofs from the same known prefix. The
snapshot, rather than a source frontier or a later materialization, is the
watermark. Ask it what representation is actually readable at that instant:

```rust,ignore
let snapshot = store.snapshot()?;
let instant = triblespace::core::clock::epoch_now();
let observed = snapshot.collection_at(collection, instant)?;
let support = observed.support();
let cover = observed.cover();
let value: V = observed.view()?;
```

`snapshot.collection_at(target, instant)` admits the foundational commits at
one explicit authorization instant, selects the
maximal complete resident target antichain, and returns only the part of the
foundational support represented by that antichain. Admitted but not yet
derived data is absent: an immutable snapshot never promises work which will
happen later. `snapshot.collection_exact(target, &support)` is the assertion
form and fails unless that exact foundational support is completely realized.
There is deliberately no hidden current-clock form: identical operations on
one frozen store snapshot must have identical results even while wall time
passes.

Both forms keep the chosen target cover inseparable from the store snapshot
which established its residency. `view` invokes `TryFromCover<E>` solely
through that frozen observation. For a `SimpleArchive`, `V = TribleSet`; for a
`SuccinctArchiveBlob`, `V` may be an mmap-backed union retaining selected
shards. `collection.read_at::<V, _>(&snapshot, instant)` remains a concise
root-collection read when the intermediate support and physical cover are
irrelevant.

Consumers which need the exact strictly verified COMMIT roots selected during
admission use `collection.admitted_with_commits_at(&snapshot, instant)`; later
attestations over the same payload remain broader provenance rather than
retroactive roots.

This is a coherent **known-prefix** observation, not a global latest
transaction. A concurrent immutable insert may appear in this call or a later
one. A mutating `ensure` or `maintain` operation returns a new store snapshot;
the caller may then ask that snapshot for the collection it actually contains.

Raw record readers still expose dangling native collection records and stored
proof records for repair. A `COMMIT`, `MERGE`, or `DERIVE` is semantically
invisible until all of its direct blob references are resident in that exact
frozen snapshot. A capability proof is already self-contained and has no blob
residency gate. Snapshot operations never acquire, wait, write, or emit
`WANT`. Record retention is a separate lifetime rule: a retained non-blob
record strongly retains every directly referenced blob which is resident, but
does not fetch an absent one; proofs simply have no such references. A `WANT`
is itself only an explicit durable demand record, never automatic cache-miss
bookkeeping.

The four live store operations are asynchronous even for local stores. They may
fetch only exact missing handles in the operation's frozen raw frontier,
publish derived work, and return a fresh snapshot; they never emit `WANT`.
Local stores implement the same contract with immediately ready acquisition
from their resident snapshot, while a networked store may await exact-H fetch.

Exact replay does not need a publishing key, re-run admission, or retain any
signed commit or metadata. The typed cover names the exact descriptor and
payload identities. Use `cover.commits(&snapshot)` when currently resident
authorship and metadata provenance matters; zero commits is a valid answer and
does not invalidate replay. Several admitted commits over the same payload are
distinct provenance fibers but one member of `Support`.

## Reuse merge work without changing meaning

A logical collection value is the join of a cover's members. It does not need
one monolithic blob. A resolver may choose members consisting of committed
payloads and stored merge results:

```text
    a       b       c           explicit payloads
     \     /        |
      a⊔b           |           reusable MERGE result
        \           /
         (a⊔b)⊔c                logical collection value
```

Distinct covers can have the same support: `{a, b}` and `{a⊔b}` are different
PATCH sets, but the stored `MERGE` equation records that they denote the same
join. This is useful for LSM-like maintenance: small commits remain
independently attributable, while deterministic merges amortize reads into
larger canonical shards. A selected target cover is replaceable computation,
never a second history or a new authority root.

## Derive another representation

Suppose `f` is a canonical join homomorphism. Its target encoding implements
`CollectionDerivation`, naming one canonical `Source` encoding and a runtime
`Argument` carried by the concrete mapping descriptor:

If a downstream crate owns neither the source nor target encoding, Rust's
orphan rule prevents that target-owned implementation. It can instead provide
an explicit `CollectionMapping` and select the same engine through
`derive_with`, `ensure_with`, and `maintain_with`.

```text
f(a ⊔ b) = f(a) ⊔ f(b)
```

Then a resolver may derive a merged source once, derive leaves separately and
merge their images, or reuse any stored mixture already present. `DERIVE`
records expose those reusable edges across collection lattices. Newly executed
joins and mappings publish every successful result and equation, even when a
later planning or storage step fails or selects another route. Publication is
operation-ordered rather than phase-batched, so a failure leaves the complete
successful prefix addressable instead of stranding its blobs without their
equations. Canonical joins, mappings, and logical cover views receive one
frozen store snapshot and may resolve immutable dependencies named by their
inputs; unrelated resident blobs are never ambient semantic input.

Succinct storage applies this model as two ordinary derivations:

```text
SimpleArchive --DERIVE--> SuccinctArchiveBlob --DERIVE-->
    Rank9AcceleratedSuccinctArchiveBlob
```

```rust,ignore
use triblespace::core::collection::{CollectionSnapshotExt, CollectionStoreExt};
use triblespace::core::blob::encodings::succinctarchive::{
    OrderedUniverse, Rank9AcceleratedSuccinctArchiveBlob, SuccinctArchiveBlob,
    UnionArchive,
};

let source = storage.collection("models", source_policy)?;
let raw = storage.derive::<SuccinctArchiveBlob>(source, (), raw_policy)?;
let accelerated = storage.derive::<Rank9AcceleratedSuccinctArchiveBlob>(
    raw,
    (),
    accelerated_policy,
)?;

let before = storage.snapshot()?;
let instant = triblespace::core::clock::epoch_now();
let support = storage
    .acquire_admitted_support_at(source, &before, instant)
    .await?;
drop(before);

// Each edge receives the same foundational Support. Work never flows upward.
storage
    .maintain_exact(raw, &support)
    .await?;
let after = storage.maintain_exact(accelerated, &support).await?;

let observed = after.collection_exact(accelerated, &support)?;
let facts: UnionArchive<OrderedUniverse> = observed.view()?;
```

- `acquire_admitted_support_at` freezes collection records and capability
  proofs at the caller's control snapshot, then acquires only the missing
  immutable descriptor, data, and metadata bytes needed to decide that
  frontier. Concurrent records and proofs remain deferred. Reusing the same
  control snapshot across several calls therefore gives a batch one semantic
  watermark without pretending that byte residency was already complete.
- `snapshot.collection_at` remains the purely read-only alternative: it
  performs no acquisition or collection algebra and binds only the maximal
  resident target cover visible in that immutable snapshot.
  `collection_exact` requires a complete realization for explicit support.
- `ensure` freezes the currently admitted foundational support, while
  `ensure_exact` accepts explicit support. Both publish only missing `DERIVE`
  work and return a fresh store snapshot.
- `maintain` and `maintain_exact` additionally carry colliding target members
  by serialized-size tier. They also return a fresh store snapshot.

An ensure may follow existing `MERGE` equations to reuse a resident
support-equivalent target decomposition, but newly executed work crosses only
the mapping. It stores each target artifact before its unsigned `DERIVE`
record. It never creates a source or target `MERGE`.

Maintenance starts from that derive-complete target cover and publishes only
horizontal target `MERGE` work. If a target join cannot run because an optional
immutable dependency is absent or the encoding has reached a capacity limit,
the finer exact target cover remains the answer. A downstream operation never
constructs an upstream member as a side effect.

The maintenance policy has no knob: a raw target member belongs to
`floor(log2(max(1, serialized_len)))`, and the lowest two content handles in
the lowest colliding tier are carried first. A capacity-limited encoding may
leave a collision stable; otherwise the resulting cover has at
most one member per tier. Pairwise-disjoint carries in one tier share a
deterministic semantic plan, but each output is constructed against a cheap
fresh store snapshot and published immediately. The exact per-point planner is
re-entered before another tier is selected. This avoids a full semantic
re-probe per pair without retaining a tier of newly generated bytes in memory.

Every position uses the same `Cover<E>` shape, but its typed handles cannot be
mixed across representations. `Cover<SimpleArchive>` contains only
`Handle<SimpleArchive>`; `Cover<SuccinctArchiveBlob>` contains only
`Handle<SuccinctArchiveBlob>`; the second stage uses
`Handle<Rank9AcceleratedSuccinctArchiveBlob>`. Stored `MERGE` equations define
support-equivalent routes; `Cover` carries no route-mode bit. Ordinary raw
Succinct derivation follows the resident-node priority above while preserving
foundational support. The accelerated stage resolves the ordinary derived
lattice over that same support. Its cover-aware view
reads each embedded raw handle through the store snapshot and validates the
exact raw/index pair before constructing the query runtime. There is no
separate member-image mode.

None of them signs a replacement root, advances a head, flushes implicitly, or
adds a special manifest. [Regular-path summaries](regular-path-indexes.md) and
Rank9 acceleration both use the same collection algebra. The accelerated
encoding is a Merkle root whose first 32 bytes name its exact portable raw
child. It is also a full lattice: resident accelerated children `A(a)` and
`A(b)` join canonically to `A(a ⊔ b)` when their exact raw union is already
resident. If that dependency is absent, maintenance keeps `{A(a), A(b)}`; a
separate upstream maintenance call may later publish the raw union, after which
a retry can carry the accelerated lattice. Each mapping or join emits exactly
one blob and then its equation. Physical resolution excludes an accelerated
member whose named raw child is unavailable and retries a finer
support-equivalent route; the typed view repeats the raw/index check at its
decoding boundary.

## WANT missing content or computation

Sparse evidence discovery deliberately does not fetch commit dependencies.
`WantStore` adds operational interest to one idempotent grow-only set with
three request shapes:

- `Blob(handle)` — obtain those exact bytes;
- `Merge(collection, low, high)` — discover an existing matching merge result;
  and
- `Derive(target, input)` — discover an existing matching derivation; the
  target descriptor already names the source collection and concrete mapping.

`Blob(H)` is the only exact-content identity. A reconciler may satisfy it from
local workers or discover providers under opaque KDF(H), without activating or
even naming a collection. The provider proves H first and the requester second,
with both proofs bound to the authenticated endpoints; H itself is never sent,
and landed bytes must hash to H. The answer to an operation WANT is the
ordinary native equation; obtaining its result bytes is a separate blob WANT.
A WANT grants no collection authority and does not change the value of any
collection. There is no `unwant` operation: cache eviction belongs to Yard's
physical rewrite policy, which re-records only surviving blob demand, while
merge and derive requests remain durable.

## Migrate a legacy branch explicitly

Old piles may contain signed commit DAGs and mutable pin records. Current
readers retain an immutable `PinSnapshot` and legacy decoders so operators can
inspect and migrate that evidence without restoring the old publication API.

```text
trible pile migrate data.pile branch-to-collection \
  --branch legacy-events \
  --collection-name events \
  --signing-key ./writer.key
```

The command is deliberately same-pile: source commit blobs must already be
resident. It freezes the selected legacy head, validates the complete reachable
DAG, and converts each authored node into a native commit using its exact
`repo::content` and `metadata::archive` handles. A missing metadata archive maps
to the canonical empty archive. Contentless merge wrappers are validated but do
not become members.

With no further options the target descriptor gives the migration signing key
one-root direct READ and WRITE policies. The resulting commits are therefore
admitted directly by ordinary collection admission against a store snapshot.

The migration-only `--authority` option instead uses another trust root for
both direct policies:

```text
trible pile migrate data.pile branch-to-collection \
  --branch legacy-events \
  --collection-name events \
  --authority <64-hex-character-ed25519-public-key> \
  --signing-key ./writer.key
```

Local publication remains unconditional, so this form still writes commits
signed by the migration key. A later read admits them only when the store holds
enough exact root-to-signer `ACTION_WRITE` evidence for the resulting
descriptor handle. The migration command does not invent, scan for, or store
that delegation.

The complete source DAG and every prepared target element are validated before
the target descriptor, dependency, or commit is published. Storage failures
remain backend errors; authorization is deliberately deferred to reads rather
than treated as permission to append locally.

Legacy wrapper parents, messages, timestamps, authors, and signatures are not
silently reinterpreted as application metadata. Two source nodes with identical
data and semantic metadata map to one intrinsic native commit. Re-running with
the same collection identity and key is idempotent.

Migration is the only reason application-facing tooling needs to name a legacy
branch. New code publishes directly to collections.

## Operational invariants

- Persist dependencies before the record that makes them meaningful.
- Treat a cover's payload identities as semantic ground truth. Availability is
  expressed as a subset in those coordinates; physical decomposition stays
  private to same-snapshot materialization. Signed commits and metadata remain
  lazy provenance queried separately.
- Treat stored unsigned equations as reusable materialized LSM work. Never
  replay algebra merely to trust a local equation; apply future trust/quorum
  policy at record admission instead.
- Persist every successful join or mapping. Yard/GC policy alone decides when
  its result bytes leave local storage.
- Keep admission, retention, and WANT policy orthogonal.
- Carry exact covers across derivation boundaries instead of asking for an
  ambient “latest”.
- Flush at explicit application durability boundaries.
- Merge stores by union; never choose meaning from append order.

These rules are sufficient for both low-latency single-process use and sparse
distributed collection maintenance without introducing a second execution
model.
