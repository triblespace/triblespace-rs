# Direct Capability Proofs

TribleSpace authorization is one direct, portable proof. Semantic restrictions
live in content-addressed claim blobs; principals and signatures live in a
compact native proof:

```text
K0 (S0 C0 K1) (S1 C1 K2) ... (Sn Cn Kn+1)
```

`K0` is an externally chosen Ed25519 trust root. Each `Si` is a signature by
`Ki`, `Ci` is the exact BLAKE3 handle of a claim blob, and `Ki+1` is the next
principal. The verifier also receives the expected final key. Nothing is
inferred from possession, storage enumeration, append order, or a mutable
membership head.

## Keyless claims

A `CapabilityClaim` is one closed canonical `SimpleArchive` containing:

| Field | Meaning |
|---|---|
| action | one exact, uninterpreted 128-bit operation ID |
| resource | one exact opaque 32-byte resource identity |
| mode | `Invoke`, `Delegate`, or `InvokeAndDelegate` |
| parent | zero or one exact parent **claim** handle |
| validity | zero or one inclusive TAI interval |

Claims contain no public key and no signature. A root claim has no parent; each
later claim names the immediately preceding claim. The same semantic claim DAG
can therefore be used in distinct principal paths without changing claim
identity.

Actions and resources are exact atoms. There are no wildcards, implicit action
hierarchies, or ambient resource namespaces in the kernel. Applications define
the conversion from a concrete resource to its 32-byte identity.

## The native proof

The canonical proof body is `K0 (S C K)+`: one 32-byte root followed by one or
more 128-byte edges. There is no count, padding, or alternate field ordering in
the body. Every edge signature covers:

```text
"triblespace.capability.proof-edge\0"
|| 1:u32be
|| issuer_key
|| claim_handle
|| delegate_key
```

Binding both keys and the exact claim handle prevents key substitution,
cross-claim replay, and path splicing. Ed25519 verification is strict. The
proof ID is BLAKE3 over the complete canonical body, so the same proof has one
stable lookup identity in memory, a pile, or another store.

`CapabilityProofStore` is a grow-only set of these native proofs. It supports
insertion, deterministic enumeration, and exact lookup by proof ID. It does
not search by key or claim and it grants no authority merely because a proof
is present.

## Verification is a meet

`CapabilityProofBundle::verify` takes four explicit boundary values:

- the external trust-root key;
- the expected leaf key, normally authenticated by the transport or named by
  the collection commit;
- the exact verification instant; and
- the requested action/resource atom and minimum mode.

It then checks the native signatures, hashes and parses the ordered claim
blobs, and evaluates the root-to-leaf restrictions:

1. the first claim has no parent;
2. each later claim names the previous claim handle;
3. the effective parent mode contains `Delegate` before another edge follows;
4. every action/resource atom is exactly equal;
5. modes combine by bit intersection; and
6. bounded validity intervals combine by inclusive intersection and contain
   the supplied instant.

This is attenuation by meet, not a syntactic “child must be narrower” rule. A
child that repeats a wider mode cannot restore a bit removed earlier; it simply
adds no restriction for that bit. An empty atom, mode, or validity meet rejects
the proof. A valid prefix cannot stand in for a descendant because verification
also checks the expected leaf key.

The result reports the effective atom, mode, validity interval, leaf claim,
leaf key, and proof ID. A holder may extend it only when its effective mode
still delegates, the signing key equals the verified leaf, and the child names
the exact leaf claim.

## Portable bundles

A `CapabilityProofBundle` carries the native proof together with the exact
claim blobs in root-to-leaf order. Its bounded version-1 transport form is:

```text
version:u8 = 1
step_count:u8
proof: 32 + step_count * 128 bytes
repeat step_count times:
    claim_length:u16be
    claim:bytes
```

The count is nonzero and at most 255. Claim lengths are bounded by the closed
canonical claim shape; decoders reject truncation, trailing bytes, noncanonical
lengths, malformed keys, and oversized outer frames before treating the bundle
as evidence. The bundle is self-contained for one verification round trip.
Possessing it does not authorize a different key because the proof and caller
both bind the expected leaf.

## Storage and lifetime

Claims are ordinary blobs. The native proof record is the direct lifetime edge
for its claim closure: conservative collection preserves every canonical proof
record, and a proof whose signatures verify makes each resident claim handle an
exact direct root. Every ancestral claim is already named explicitly by the
proof, so the collector does not scan opaque claim values or follow parent
handles recursively. A missing claim remains missing and can be fetched later;
an invalidly signed proof roots no blob. Trust-root selection and semantic
claim verification remain caller responsibilities, not garbage-collector
policy.

There is no second retention collection. Storing a proof and its claims is
enough. The storage layer publishes claim blobs before the proof record so an
observer never mistakes a partially written local bundle for complete local
evidence.

## Collection-local admission

Collection descriptors carry two independent policies. Each policy is either
open or a quorum over a canonical set of Ed25519 roots, with separate invoke
and optional delegation thresholds:

```text
READ(C)  = CapabilityAtom(ACTION_READ,  resource = C)
WRITE(C) = CapabilityAtom(ACTION_WRITE, resource = C)
```

The resource is the exact 32-byte collection descriptor handle. There is no
ambient team, owner field, wildcard namespace, or transport-wide inventory
grant. A derived collection states its own policies; neither source ancestry
nor possession of another collection's proof implies anything about it.

A policy root contributes inherent support. Other principals contribute only
through strictly verified proof paths beginning at canonical policy roots.
Quorum evaluation unions the distinct roots which support the requested leaf
at one instant; two paths from the same root do not count twice. Invoke and
delegate support are evaluated separately, so permission to use an action need
not permit issuing another grant.

WRITE(C) decides which signed COMMITs are active when a store snapshot is
observed. Local insertion remains unconditional: a store may retain an inactive
commit, and later proof evidence may activate it monotonically. READ(C) is the
collection-evidence disclosure boundary. A collection repair server evaluates
the TLS-authenticated endpoint against complete READ(C) paths already in its
pinned collection-scoped authorization projection. The request may additionally
carry bounded native proofs for cold bootstrap. They cannot admit the current
immutable session: the server stores them inertly and evaluates them only from
a later coherent snapshot. Merely receiving a proof neither fetches nor
asserts demand for the claim blobs it names. An actual consumer which follows
a missing claim handle resolves H through the ordinary blob data plane, exactly
like every other content-addressed dependency.
Collection repair likewise carries every structurally valid signed COMMIT(C)
without asking the sender to prove WRITE. The receiver derives activity from
its own snapshot, so a grant and its commit may arrive in either order and the
publisher need never receive its own grant.
Exact blob retrieval is orthogonal to collection admission. Every served
resident H may publish only its domain-separated locator KDF(H) and an H-bound
endpoint token; directory nodes never receive H. On the exact stream the
provider proves H first and the requester second, with both proofs bound to the
authenticated endpoint identities. H is never transmitted, and the requester
hash-verifies the returned bytes. READ(C) is therefore neither required nor
consulted by exact GET.

This is a bearer capability, not an entropy amplifier. KDF(H) and the proof
tokens avoid disclosing H but do not make guessable plaintext secret: anyone
who can guess the bytes can compute H and exercise the same capability. Data
whose content must not be guessable should be randomized or encrypted before
content addressing.

The iroh connection itself already authenticates endpoint keys. TribleSpace
therefore adds no generic CONNECT capability and no second team-inventory
session. Every collection request is authorized against the exact descriptor
it names.

## WANT and synchronization boundaries

Capability proof records are durable local set evidence, but there is no global
proof inventory or claim-specific WANT. Collection-scoped repair
projects native proof records whose complete resident claim closures are
structurally relevant to exact READ(C) or WRITE(C). Its PATCH leaves contain
only canonical proof bytes. Proof receipt does not fetch or WANT its referenced
claims. If later evaluation follows a missing claim handle, it resolves H
through the generic H-addressed blob data plane; an explicit WANT may delegate
durable fulfillment but is never protocol bookkeeping. Portable bundles remain
application-level invitation artifacts, not a network representation.

This separation is intentional:

- proof presence is not authority;
- routing, gossip, or DHT presence is not authorization;
- WANT is local demand, not authorization; and
- blob availability is not semantic validity.

## Bootstrap and grants

Collection bootstrap is descriptor construction. The creator chooses READ and
WRITE policies explicitly, registers the canonical descriptor, and retains the
root signing keys needed by those policies. An open action needs no grant. A
root acting directly needs no stored proof. A delegated principal receives a
self-contained proof bundle whose atom names the exact action and collection.

The recipient verifies the bundle against the descriptor's externally known
root set and its own expected key before storing it. The artifact therefore
cannot nominate its own authority, collection, or subject. There is no global
roster to enumerate and no requirement that two collections share roots or
delegation geometry.

`grant_collection_read(&mut store, collection, &root, recipient)` and
`grant_collection_write(&mut store, collection, &root, recipient)` are the
root-only persistence seams for the common direct cases. Each validates the
exact descriptor and matching action root against one snapshot, creates an
unbounded Invoke bundle, stores its claim closure, and only then inserts the
native proof record. Repeating the same inputs reproduces the same identities.
Under a threshold policy, invoke the helper once for each distinct root whose
support should participate in the quorum.

`collection_read_audience_at(&snapshot, collection, instant)` evaluates the
restricted READ forest once and returns every currently admitted root,
intermediate delegate, and leaf in canonical key order. It returns
`CollectionReadAudience::Open` for open READ, because no finite principal list
can represent that audience; incomplete claim closure and invalid-at-instant
paths remain inert.

The equivalent pile operations are:

```text
trible pile collection grant-read PILE COLLECTION RECIPIENT [--key PATH]
trible pile collection grant-write PILE COLLECTION RECIPIENT [--key PATH]
```

`RECIPIENT` is an Ed25519 public key in the same hex or z-base-32 spelling as
an iroh endpoint id. Each command refuses an open policy (which needs no
proof) and keys absent from the descriptor's roots for that action.

Validity bounds are optional monotone restrictions, not mutable revocation.
Ending an unexpired grant requires changing the served trust root or another
explicit application epoch. That cost keeps the kernel local, portable, and
independent of a second authorization database.
