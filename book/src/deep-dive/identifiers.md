# Identifiers for Distributed Systems

Distributed systems are assembled from independently authored pieces of data.
Keeping those pieces addressable requires names that survive replication,
concurrent edits, and local conventions. We have found it useful to categorize
identifier schemes along two axes:

|                | **Abstract**            | **Semantic**        |
|----------------|-------------------------|---------------------|
| **Intrinsic**  | Hash, Signature, PubKey | Embeddings          |
| **Extrinsic**  | UUID, UFOID, FUCID      | Names, DOI, URL     |

The rows describe how an identifier is minted while the columns describe what
it tries to communicate. Classifying an identifier along both axes makes its
trade-offs explicit and helps decide how to combine multiple schemes for a
given workflow.

## Abstract vs. Semantic Identifiers

### Semantic identifiers

Semantic identifiers (names, URLs, descriptive labels, embeddings) carry meaning
about the thing they reference. That context makes them convenient for humans
and for search workflows where the identifier itself narrows the scope of
possible matches. Their usefulness comes with caveats:

- Semantics drift. A name that once felt accurate can become misleading as an
  entity evolves.
- Semantic identifiers are rarely unique. They work best as entry points into a
  richer dataset rather than as the source of truth for identity.
- Without scoping, semantics clash. Two communities can reasonably reuse the
  same word for different concepts, so the identifier must be tied to a
  namespace, deployment, or audience.

Distributed systems reconcile those tensions by mapping many local semantic
names to a persistent abstract identifier. Users remain free to adopt whatever
terminology makes sense to them while the system maintains a shared canonical
identity.

Embeddings deserve a special mention. They encode meaning in a machine-friendly
form that can be compared for similarity instead of exact equality. That makes
them great for recommendations and clustering but still unsuitable as primary
identifiers: two distinct entities can legitimately share similar embeddings,
and embeddings can change whenever the underlying model is retrained.

### Abstract identifiers

Abstract identifiers (UUIDs, UFOIDs, FUCIDs, hashes, signatures) strip all
meaning away in favor of uniqueness. They can be minted without coordination,
usually by drawing from a high-entropy space and trusting probability to keep
collisions effectively impossible. Abstract identifiers shine when you need:

- Stable handles that survive across replicas and through refactors.
- Globally unique names without a centralized registrar.
- Cheap, constant-time generation so every component can allocate identifiers on
  demand.

Because they carry no inherent semantics, abstract identifiers are almost always
paired with richer metadata. They provide the skeleton that keeps references
consistent while semantic identifiers supply the narrative that humans consume.

## Intrinsic vs. Extrinsic Identifiers

The intrinsic/extrinsic axis captures whether an identifier can be recomputed
from the entity itself or whether it is assigned externally.

### Intrinsic identifiers

Intrinsic identifiers (cryptographic hashes, digital signatures, content-based
addresses) are derived from the bytes they describe. They function as
fingerprints: if two values share the same intrinsic identifier then they are
bit-for-bit identical. This property gives us:

- Immutability. Changing the content produces a different identifier, which
  immediately signals tampering or corruption.
- Self-validation. Replicas can verify received data locally instead of trusting
  a third party.
- Stronger adversarial guarantees. Because an attacker must find collisions
  deliberately, intrinsic identifiers rely on cryptographic strength rather than
  purely statistical rarity.

### Extrinsic identifiers

Extrinsic identifiers (names, URLs, DOIs, UUIDs, UFOIDs, FUCIDs) are assigned by
policy instead of by content. They track a conceptual entity as it evolves
through versions, formats, or migrations. In other words, extrinsic identifiers
carry the "story" of a thing while intrinsic identifiers nail down individual
revisions.

Thinking about the classic ship of Theseus thought experiment makes the
distinction concrete: the restored ship and the reconstructed ship share the
same extrinsic identity (they are both "Theseus' ship") but have different
intrinsic identities because their planks differ.

## Embeddings as Semantic Intrinsic Identifiers

Embeddings blur our neat taxonomy. They are intrinsic because they are computed
from the underlying data, yet they are overtly semantic because similar content
produces nearby points in the embedding space. That duality makes them powerful
for discovery:

- Systems can exchange embeddings as a "lingua franca" without exposing raw
  documents.
- Expensive feature extraction can happen once and power many downstream
  indexes, decentralizing search infrastructure.
- Embeddings let us compare otherwise incomparable artifacts (for example, a
  caption and an illustration) by projecting them into a shared space.

Despite those advantages, embeddings should still point at a durable abstract
identifier rather than act as the identifier. Collisions are expected, model
updates can shift the space, and floating-point representations can lose
determinism across hardware.

## High-Entropy Identifiers

For a truly distributed system, the creation of identifiers must avoid the bottlenecks and overhead associated
with a central coordinating authority. At the same time, we must ensure that these identifiers are unique.  

To guarantee uniqueness, we use abstract identifiers containing a large amount of entropy, making collisions
statistically irrelevant. However, the entropy requirements differ based on the type of identifier:
- **Extrinsic abstract identifiers** need enough entropy to prevent accidental collisions in normal operation.
- **Intrinsic abstract identifiers** must also resist adversarial forging attempts, requiring significantly higher entropy.  

From an information-theoretic perspective, the length of an identifier determines the maximum amount of
entropy it can encode. For example, a 128-bit identifier can represent \( 2^{128} \) unique values, which is
sufficient to make collisions statistically negligible even for large-scale systems.  

For intrinsic identifiers, 256 bits is widely considered sufficient when modern cryptographic hash functions
(e.g., SHA-256) are used. These hash functions provide strong guarantees of collision resistance, preimage
resistance, and second-preimage resistance. Even in the event of weaknesses being discovered in a specific
algorithm, it is more practical to adopt a new hash function than to increase the bit size of identifiers.  

Additionally, future advances such as quantum computing are unlikely to undermine this length. Grover's algorithm
would halve the effective security of a 256-bit hash, reducing it to \( 2^{128} \) operations—still infeasible with
current or theoretical technology. As a result, 256 bits remains a future-proof choice for intrinsic identifiers.  

Such 256-bit intrinsic identifiers are represented by the types
[`triblespace::core::value::schemas::hash::Hash`](crate::value::schemas::hash::Hash) and
[`triblespace::core::value::schemas::hash::Handle`](crate::value::schemas::hash::Handle).  

Not every workflow needs cryptographic strength. We therefore ship three
high-entropy abstract identifier families—**RNGID, UFOID, and FUCID**—that keep
128 bits of global uniqueness while trading off locality, compressibility, and
predictability to suit different scenarios.

## Comparison of Identifier Types

|                        | [RNGID](crate::id::rngid::rngid) | [UFOID](crate::id::ufoid::ufoid) | [FUCID](crate::id::fucid::fucid) |
|------------------------|----------------------------------|----------------------------------|----------------------------------|
| Global entropy         | 128 bits                        | 96 bits random + timestamp       | 128 bits                         |
| Locality               | None                            | High (time-ordered)              | High (monotonic counter)         |
| Compression friendliness | None                          | Low                              | High                             |
| Predictability         | None                            | Low (reveals mint time)          | High (per-source sequence)       |

# Example: Scientific Publishing

Consider the case of published scientific papers. Each artifact, such as a `.html`
or `.pdf` file, should be identified by its abstract intrinsic identifier,
typically a cryptographic hash of its content. This ensures that any two
entities referencing the same hash are referring to the exact same version of
the artifact, providing immutability and validation.

Across different versions of the same paper, an abstract extrinsic identifier can
tie these artifacts together as part of one logical entity. The identifier
provides continuity regardless of changes to the paper’s content over time.

Semantic (human-readable) identifiers, such as abbreviations in citations or
bibliographies, are scoped to individual papers and provide context-specific
usability for readers. These names do not convey identity but serve as a way for
humans to reference the persistent abstract identifiers that underlie the
system.

Sadly the identifiers used in practice, such as DOIs, fail to align with these
principles and strengths. They attempt to provide global extrinsic semantic
identifiers for scientific papers, an ultimately flawed approach. They lack the
associated guarantees of intrinsic identifiers and bring all the challenges of
semantic identifiers. With their scope defined too broadly and their authority
centralized, they fail to live up to the potential of distributed systems.

# ID Ownership

In distributed systems, consistency requires monotonicity due to the CALM principle.
However, this is not necessary for single writer systems. By assigning each ID an owner,
we ensure that only the current owner can write new information about an entity associated
with that ID. This allows for fine-grained synchronization and concurrency control.

To create a transaction, you can uniquely own all entities involved and write new data for them
simultaneously. Since there can only be one owner for each ID at any given time, you can be
confident that no other information has been written about the entities in question.

By default, all minted `ExclusiveId`s are associated with the thread they are dropped from.
These IDs can be found in queries via the `local_ids` function.

Once the IDs are back in scope you can either work with them directly as
[`ExclusiveId`](crate::id::ExclusiveId)s or move them into an explicit
[`IdOwner`](crate::id::IdOwner) for a longer lived transaction.  The example
below shows both approaches in action:

```rust
use triblespace::examples::literature;
use triblespace::prelude::*;

let mut kb = TribleSet::new();
{
    let isaac = ufoid();
    let jules = ufoid();
    kb += entity! { &isaac @
        literature::firstname: "Isaac",
        literature::lastname: "Asimov",
    };
    kb += entity! { &jules @
        literature::firstname: "Jules",
        literature::lastname: "Verne",
    };
} // `isaac` and `jules` fall back to this thread's implicit IdOwner here.

let mut txn_owner = IdOwner::new();
let mut updates = TribleSet::new();

for (author, name) in find!(
    (author: ExclusiveId, name: String),
    and!(
        local_ids(author),
        pattern!(&kb, [{
            ?author @ literature::firstname: ?name
        }])
    )
) {
    // `author` is an ExclusiveId borrowed from the implicit thread owner.
    let author_id = txn_owner.insert(author);

    {
        let borrowed = txn_owner
            .borrow(&author_id)
            .expect("the ID was inserted above");
        updates += entity! { &borrowed @ literature::lastname: name.clone() };
    } // `borrowed` drops here and returns the ID to `txn_owner`.
}
```

Sometimes you want to compare two attributes without exposing the comparison
variable outside the pattern. Prefixing the binding with `_?`, such as
`_?name`, allocates a scoped variable local to the macro invocation. Both
`pattern!` and `pattern_changes!` will reuse the same generated query variable
whenever the `_?` form appears again, letting you express equality constraints
inline without touching the outer [`find!`](crate::query::find) signature.

Binding the variable as an [`ExclusiveId`](crate::id::ExclusiveId) means the
closure that [`find!`](crate::query::find) installs will run the
[`FromValue`](crate::value::FromValue) implementation for `ExclusiveId`.
`FromValue` simply unwraps [`TryFromValue`](crate::value::TryFromValue), which
invokes [`Id::aquire`](crate::id::Id::aquire) and would panic if the current
thread did not own the identifier.  The
[`local_ids`](crate::query::local_ids) constraint keeps the query safe by only
enumerating IDs already owned by this thread.  In the example we immediately
move the acquired guard into `txn_owner`, enabling subsequent calls to
[`IdOwner::borrow`](crate::id::IdOwner::borrow) that yield
[`OwnedId`](crate::id::OwnedId)s.  Dropping an `OwnedId` automatically returns
the identifier to its owner so you can borrow it again later.  If you only need
the ID for a quick update you can skip the explicit owner entirely, bind the
variable as a plain [`Id`](crate::id::Id), and call
[`Id::aquire`](crate::id::Id::aquire) when exclusive access is required.

## Ownership and Eventual Consistency

While a simple grow set like the history stored in a [Head](crate::remote::Head)
already constitutes a conflict-free replicated data type (CRDT), it is also limited in expressiveness.
To provide richer semantics while guaranteeing conflict-free mergeability we allow only
"owned" IDs to be used in the `entity` position of newly generated triples.
As owned IDs are [Send] but not [Sync] owning a
set of them essentially constitutes a single writer transaction domain,
allowing for some non-monotonic operations like `if-does-not-exist`, over
the set of contained entities. Note that this does not make operations that
would break CALM (consistency as logical monotonicity) safe, e.g. `delete`.


