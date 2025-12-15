# Blobs

Blobs are immutable sequences of bytes used whenever data no longer fits into
the fixed 256‑bit value slot of a trible. Instead of treating these payloads as
untyped binary blobs, Tribles keeps track of their structure via `BlobSchema`.
Much like `ValueSchema` drives how values are serialized into a trible, a
`BlobSchema` defines how to encode and decode rich data into a byte sequence.

## When to reach for blobs

Values and tribles capture compact facts – identifiers, timestamps, counters –
in a fixed width. Whenever information grows beyond that footprint, blobs carry
the payload while tribles continue to reference it. Common use cases include
documents, media assets, serialized entity archives, or even domain specific
binary formats. Because blobs are content addressed, the same payload stored
twice automatically deduplicates to the same handle. In the in-memory
implementation this falls straight out of the code: `MemoryBlobStore::insert`
keeps a `BTreeMap` keyed by the handle and simply reuses the existing entry
when the same digest shows up again.

## Handles, schemas, and stores

Blobs live in a `BlobStore`. The store provides persistent storage and a
content hash, determined by the selected `HashProtocol`, that acts as a stable
handle. Handles can be embedded into tribles just like any other value so they
benefit from the existing querying machinery. A handle couples the blob's hash
with its `BlobSchema` so consumers always know how to deserialize the
referenced bytes.

Converting Rust types to blobs is infallible in practice, therefore the `ToBlob`
and `TryFromBlob` traits are the most common helpers. The `TryToBlob` and
`FromBlob` variants have been dropped to keep the API surface small without
losing ergonomics.

## End‑to‑end example

The following example demonstrates creating blobs, archiving a `TribleSet` and
signing its contents:

```rust
use triblespace::prelude::*;
use triblespace::examples::literature;
use triblespace::repo;
use valueschemas::{Handle, Blake3};
use blobschemas::{SimpleArchive, LongString};
use rand::rngs::OsRng;
use ed25519_dalek::{Signature, Signer, SigningKey};

// Build a BlobStore and fill it with some data.
let mut memory_store: MemoryBlobStore<Blake3> = MemoryBlobStore::new();

let book_author_id = fucid();
let quote_a: Value<Handle<Blake3, LongString>> = memory_store
    .put("Deep in the human unconscious is a pervasive need for a logical universe that makes sense. But the real universe is always one step beyond logic.")
    .unwrap();
let quote_b = memory_store
    .put("I must not fear. Fear is the mind-killer. Fear is the little-death that brings total obliteration. I will face my fear. I will permit it to pass over me and through me. And when it has gone past I will turn the inner eye to see its path. Where the fear has gone there will be nothing. Only I will remain.")
    .unwrap();

let set = entity!{
   literature::title: "Dune",
   literature::author: &book_author_id,
   literature::quote: quote_a,
   literature::quote: quote_b
};

// Serialize the TribleSet and store it as another blob. The resulting
// handle points to the archived bytes and keeps track of its schema.
let archived_set_handle: Value<Handle<Blake3, SimpleArchive>> = memory_store.put(&set).unwrap();

let mut csprng = OsRng;
let commit_author_key: SigningKey = SigningKey::generate(&mut csprng);
let signature: Signature = commit_author_key.sign(
    &memory_store
        .reader()
        .unwrap()
        .get::<Blob<SimpleArchive>, SimpleArchive>(archived_set_handle)
        .unwrap()
        .bytes,
);

// Store the handle in another TribleSet so the archived content can be
// referenced alongside metadata and cryptographic proofs.
let _meta_set = entity!{
   repo::content: archived_set_handle,
   repo::short_message: "Initial commit",
   repo::signed_by: commit_author_key.verifying_key(),
   repo::signature_r: signature,
   repo::signature_s: signature,
};
```

Blobs complement tribles and values by handling large payloads while keeping the
core data structures compact. Embedding handles into entities ties together
structured metadata and heavyweight data without breaking immutability or
introducing duplication. This division of labor lets tribles focus on querying
relationships while BlobStores take care of storage concerns such as hashing,
deduplication, and retrieval.
