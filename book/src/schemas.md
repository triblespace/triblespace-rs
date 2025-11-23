# Schemas

Trible Space stores data in strongly typed values and blobs. A *schema*
describes the language‑agnostic byte layout for these types: [`Value`]s always
occupy exactly 32&nbsp;bytes while [`Blob`]s may be any length. Schemas translate
those raw bytes to concrete application types and decouple persisted data from a
particular implementation. This separation lets you refactor to new libraries or
frameworks without rewriting what's already stored or coordinating live
migrations. The crate ships with a collection of ready‑made schemas located in
[`triblespace::core::value::schemas`](https://docs.rs/triblespace/latest/triblespace/core/value/schemas/index.html) and
[`triblespace::core::blob::schemas`](https://docs.rs/triblespace/latest/triblespace/core/blob/schemas/index.html).

When data crosses the FFI boundary or is consumed by a different language, the
schema is the contract both sides agree on. Consumers only need to understand
the byte layout and identifier to read the data—they never have to link against
your Rust types. Likewise, the Rust side can evolve its internal
representations—add helper methods, change struct layouts, or introduce new
types—without invalidating existing datasets.

### Why 32 bytes?

Storing arbitrary Rust types requires a portable representation. Instead of
human‑readable identifiers like RDF's URIs, Tribles uses a fixed 32‑byte array
for all values. This size provides enough entropy to embed intrinsic
identifiers—typically cryptographic hashes—when a value references data stored
elsewhere in a blob. Keeping the width constant avoids platform‑specific
encoding concerns and makes it easy to reason about memory usage.

### Conversion traits

Schemas define how to convert between raw bytes and concrete Rust types. The
conversion traits `ToValue`/`FromValue` and their fallible counterparts live on
the schema types rather than on `Value` itself, avoiding orphan‑rule issues when
supporting external data types. The `Value` wrapper treats its bytes as opaque;
schemas may validate them or reject invalid patterns during conversion.

Fallible conversions (`TryFromValue` / `TryToValue`) are particularly useful for
schemas that must validate invariants, such as checking that a timestamp falls
within a permitted range or ensuring reserved bits are zeroed. Returning a
domain‑specific error type keeps validation logic close to the serialization
code.

```rust
use tribles::value::schemas::shortstring::ShortString;
use tribles::value::{TryFromValue, TryToValue, Value};

struct Username(String);

impl TryToValue<ShortString> for Username {
    type Error = &'static str;

    fn try_to_value(&self) -> Result<Value<ShortString>, Self::Error> {
        if self.0.is_empty() {
            Err("username must not be empty")
        } else {
            self.0
                .as_str()
                .try_to_value::<ShortString>()
                .map_err(|_| "username too long or contains NULs")
        }
    }
}

impl TryFromValue<'_, ShortString> for Username {
    type Error = &'static str;

    fn try_from_value(value: &Value<ShortString>) -> Result<Self, Self::Error> {
        String::try_from_value(value)
            .map(Username)
            .map_err(|_| "invalid utf-8 or too long")
    }
}
```

### Schema identifiers

Every schema declares a unique 128‑bit identifier via the shared
`ConstMetadata::id()` hook (for example, `<ShortString as ConstMetadata>::id()`).
Persisting these IDs keeps serialized data self describing so other tooling can
make sense of the payload without linking against your Rust types. Dynamic
language bindings (like the Python crate) inspect the stored schema identifier
to choose the correct decoder, while internal metadata stored inside Trible
Space can use the same IDs to describe which schema governs a value, blob, or
hash protocol.

Identifiers also make it possible to derive deterministic attribute IDs when you
ingest external formats. Helpers such as `Attribute::<S>::from_name("field")`
combine the schema ID with the source field name to create a stable attribute so
re-importing the same data always targets the same column.

## Built‑in value schemas

The crate provides the following value schemas out of the box:
- `GenId` &ndash; an abstract 128 bit identifier.
- `ShortString` &ndash; a UTF-8 string up to 32 bytes.
- `U256BE` / `U256LE` &ndash; 256-bit unsigned integers.
- `I256BE` / `I256LE` &ndash; 256-bit signed integers.
- `R256BE` / `R256LE` &ndash; 256-bit rational numbers.
- `F256BE` / `F256LE` &ndash; 256-bit floating point numbers.
- `Hash` and `Handle` &ndash; cryptographic digests and blob handles (see [`hash.rs`](../src/value/schemas/hash.rs)).
- `ED25519RComponent`, `ED25519SComponent` and `ED25519PublicKey` &ndash; signature fields and keys.
- `NsTAIInterval` to encode time intervals.
- `UnknownValue` as a fallback when no specific schema is known.

```rust
use triblespace::core::metadata::ConstMetadata;
use triblespace::core::value::schemas::shortstring::ShortString;
use triblespace::core::value::{ToValue, ValueSchema};

let v = "hi".to_value::<ShortString>();
let raw_bytes = v.raw; // Persist alongside the schema's metadata id.
let schema_id = <ShortString as ConstMetadata>::id();
```

## Built‑in blob schemas

The crate also ships with these blob schemas:

- `LongString` for arbitrarily long UTF‑8 strings.
- `SimpleArchive` which stores a raw sequence of tribles.
- `SuccinctArchiveBlob` which stores the [`SuccinctArchive` index
  type](https://docs.rs/tribles/latest/tribles/blob/schemas/succinctarchive/struct.SuccinctArchive.html)
  for offline queries. The `SuccinctArchive` helper exposes high-level
  iterators while the `SuccinctArchiveBlob` schema is responsible for the
  serialized byte layout.
- `UnknownBlob` for data of unknown type.

```rust
use triblespace::metadata::ConstMetadata;
use triblespace::blob::schemas::longstring::LongString;
use triblespace::blob::{Blob, BlobSchema, ToBlob};

let b: Blob<LongString> = "example".to_blob();
let schema_id = <LongString as ConstMetadata>::id();
```

## Defining new schemas

Custom formats implement [`ValueSchema`] or [`BlobSchema`].  A unique identifier
serves as the schema ID.  The example below defines a little-endian `u64` value
schema and a simple blob schema for arbitrary bytes.

```rust
{{#include ../../examples/custom_schema.rs:custom_schema}}
```

See [`examples/custom_schema.rs`](https://github.com/triblespace/triblespace-rs/blob/main/examples/custom_schema.rs) for the full
source.

### Versioning and evolution

Schemas form part of your persistence contract. When evolving them consider the
following guidelines:

1. **Prefer additive changes.** Introduce a new schema identifier when breaking
   compatibility. Consumers can continue to read the legacy data while new
   writers use the replacement ID.
2. **Annotate data with migration paths.** Store both the schema ID and a
   logical version number if the consumer needs to know which rules to apply.
   `UnknownValue`/`UnknownBlob` allow you to safely defer decoding until a newer
   binary is available.
3. **Keep validation centralized.** Place invariants in your schema
   conversions so migrations cannot accidentally create invalid values.

By keeping schema identifiers alongside stored values and blobs you can roll out
new representations incrementally: ship readers that understand both IDs, update
your import pipelines, and finally switch writers once everything recognizes the
replacement schema.
