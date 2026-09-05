# Resource Capability Proofs

TribleSpace authorization is a self-contained, prefix-signed path. A proof
names one opaque resource and carries one trust root's authority through one
or more delegates:

```text
magic | resource | root |
    (action | mode/validity flags | validity | delegate | signature)+
```

The high-entropy magic identifies this exact grammar. An incompatible grammar
gets a new magic rather than a version branch inside the decoder. `resource`
is an uninterpreted 32-byte identity: collections use their descriptor handle,
while another subsystem may give the same kernel a different kind of resource.
`root` and every delegate are canonical, non-weak Ed25519 public keys. This
makes each exact 32-byte value one usable principal rather than allowing
multiple encodings of the same curve point to count as distinct quorum roots.

Each edge contains one exact 128-bit action, an invocation/delegation mode, an
optional inclusive TAI interval, the next delegate, and a signature by the
current issuer. The signature comes last and signs every preceding byte of the
proof through that delegate. It therefore covers the magic, resource, root,
all earlier edges including their signatures, and the current restrictions.
Edges cannot be reordered, grafted onto another path, or moved to another
resource without invalidating a signature.

Every exact byte prefix ending after a signature is itself a complete proof.
A valid longer proof consequently also provides evidence for its intermediate
delegates. Truncating at any other byte is malformed. A stored value with a
malformed or invalid tail is inert as a whole; a sender that wants to publish
the valid prefix publishes those exact prefix bytes under their own content
identity.

## Canonical wire value

The proof header is 80 bytes:

| bytes | field |
|---:|---|
| 16 | grammar magic |
| 32 | opaque resource identity |
| 32 | root public key |

Every edge is 145 bytes:

| bytes | field |
|---:|---|
| 16 | exact action ID |
| 1 | mode and validity-presence flags |
| 32 | signed inclusive validity bounds, or canonical zeros when absent |
| 32 | delegate public key |
| 64 | Ed25519 signature over the complete preceding prefix |

There is no count, padding, parent pointer, claim handle, or alternate field
order in the proof body. Its exact length determines the nonzero edge count,
which is bounded at 255. Decoding rejects unknown flag bits, nil actions,
malformed, noncanonical, or weak keys, inverted intervals, nonzero
absent-validity bytes, trailing bytes, and overlong paths. Signature
verification is strict. The proof ID is BLAKE3 over the exact canonical body.

The old `K(S,C,K)+` format separated semantic restrictions into claim blobs.
That indirection is gone. A proof no longer depends on a blob closure, needs no
portable bundle wrapper, and can be verified or repaired as one value.

## Attenuation is a meet

The three nonempty modes are `Invoke`, `Delegate`, and
`InvokeAndDelegate`. Effective authority is the meet of every edge:

- the action must remain exactly equal;
- mode bits combine by intersection;
- validity intervals combine by inclusive intersection; and
- every non-final issuer must still have effective `Delegate` authority.

A syntactically wider child is harmless: it cannot restore a mode bit or time
range removed by an ancestor. An empty mode or interval intersection rejects
the path. Verification additionally receives the expected trust root, subject,
instant, resource/action request, and required mode; the proof cannot nominate
those boundary values on the verifier's behalf.

`CapabilityProof::issue_root` creates the first signed edge.
`CapabilityProof::extend` appends an edge only when the supplied signing key is
the current leaf and the effective prefix still delegates. A
`VerifiedCapability` reports the subject and effective restrictions and can be
used as the same extension boundary.

## Quorum is independent root paths

One proof carries exactly one root's share. For a policy with roots
`{r1, r2, ...}` and threshold `t`, a subject is admitted when at least `t`
distinct configured roots independently provide a valid prefix for that
subject and request at the chosen instant. Two paths from one root never count
twice. A configured root inherently supplies its own share.

There is no fixed-point authority forest and no way for one sibling proof to
lend delegation support to another. If a threshold-authorized delegate wants
to delegate onward, it extends each independently rooted proof path it holds.
The edge's signed mode states whether that particular share remains
delegable; the descriptor needs no second delegation threshold.

This path-local rule makes proof arrival order irrelevant. Set union of proofs
is the only synchronization operation, and evaluating the same set at the same
instant produces the same authority.

## Storage and lifetime

`CapabilityProofStore` is a grow-only set of canonical proof values. It offers
insertion, deterministic enumeration, and exact lookup by proof ID. Storage is
evidence, not authority: callers still choose roots, request, subject, and
instant when verifying it.

A proof is a native record and has no blob references. Conservative collection
retains the proof record and its record-kind description, but does not invent a
blob lifetime edge. Re-inserting identical proof bytes is idempotent.

The proof grammar's magic, the pile record kind, and the network protocol are
separate compatibility boundaries. The new proof body uses a fresh pile record
kind; the previous development-only proof kind remains recognizable as inert
so old append-only piles can still be crossed safely. Old signatures cannot be
mechanically migrated because they signed different bytes. Delegated authority
must be reissued by the relevant private keys.

## Collections consume authority

A collection descriptor supplies independent READ and WRITE policies. Each is
open or a quorum over a canonical root set:

```text
READ(C)  = action ACTION_READ  over resource C
WRITE(C) = action ACTION_WRITE over resource C
```

Here `C` is the exact descriptor handle. The generic capability kernel does not
know what a collection, team, secret, or query is. Collection admission merely
interprets its own resource and action IDs.

WRITE admission decides which strictly signed COMMIT records contribute to an
observed collection. Local insertion remains unconditional: an inactive commit
may arrive before its proof and become visible monotonically when enough proof
paths arrive. READ admission decides which authenticated peers may participate
in that collection's repair session. Exact blob retrieval remains orthogonal;
knowledge of a blob handle is the read capability for those bytes.

The common root-grant helpers are:

```text
grant_collection_read(&mut store, collection, &root, recipient)
grant_collection_write(&mut store, collection, &root, recipient)
```

They validate the descriptor and matching root against one snapshot, create an
unbounded `Invoke` proof, and insert that one proof record. Open policies need
no proof. Under a threshold policy, each participating root issues its own
proof. More elaborate delegation uses the capability API directly.

`collection_read_audience(&snapshot, collection)` evaluates every stored proof
at the snapshot's frozen instant and
returns the finite restricted audience in canonical key order, including valid
intermediate delegates. It returns `Open` for an open READ policy because no
finite key list describes that audience.

## Repair and discovery

Authorization repair exchanges proof records only. There are no claim blobs,
proof bundles, claim-specific WANTs, or hidden out-of-band closure. A collection
session projects only proofs relevant to its descriptor's READ or WRITE action;
the receiver applies its own policy and instant.

This leaves the layers deliberately independent:

- proof presence is not authority;
- routing, gossip, and DHT presence are not authority;
- collection repair moves records, not payload closure;
- WANT records local durable demand, not protocol bookkeeping; and
- blob residency is not semantic validity.

Validity bounds are monotone restrictions, not mutable revocation. Ending
unexpired authority requires changing the trusted policy epoch or another
explicit application-level act; ordinary expiry merely makes the same durable
proof inactive after its signed upper bound.
