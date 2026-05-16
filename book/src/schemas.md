# Schemas

TribleSpace stores data in strongly typed values and blobs. A *schema*
describes the language‑agnostic byte layout for these types: [`Inline`]s always
occupy exactly 32&nbsp;bytes while [`Blob`]s may be any length. Schemas translate
those raw bytes to concrete application types and decouple persisted data from a
particular implementation. This separation lets you refactor to new libraries or
frameworks without rewriting what's already stored or coordinating live
migrations. The crate ships with a collection of ready‑made schemas located in
[`triblespace::core::inline::encodings`](https://docs.rs/triblespace/latest/triblespace/core/inline/encodings/index.html) and
[`triblespace::core::blob::encodings`](https://docs.rs/triblespace/latest/triblespace/core/blob/encodings/index.html).

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

Conversion goes through the `Encodes<Source>` trait, which lives **on the
schema** (the schema is the impl target; the source is the trait parameter).
This is the same direction as std's `From<T>` — and for the same reason: it
trivially satisfies Rust's orphan rule, so you can write
`impl Encodes<SomeForeignType> for MyLocalSchema` without any "trait
position 0" gymnastics.

The ergonomic source-side methods `.to_inline()` / `.to_blob()` /
`.into_encoded()` are auto-derived blanket implementations — users never
implement them directly, the same way you never implement `Into<T>` in Rust:

```text
User implements:     Auto-derived via blanket:
  Encodes<T> for S     IntoEncoded<S> for T  (+ IntoInline / IntoBlob aliases)
```

For fallible conversions where the error type is part of the contract (parsing
a hex string into a hash, validating a timestamp range, rejecting reserved
bits), use `TryToInline` / `TryFromInline` — kept as separate traits because the
error type is per‑source.

```rust
use triblespace::core::inline::encodings::shortstring::ShortString;
use triblespace::core::inline::{TryFromInline, TryToInline, Inline};

struct Username(String);

impl TryToInline<ShortString> for Username {
    type Error = &'static str;

    fn try_to_inline(self) -> Result<Inline<ShortString>, Self::Error> {
        if self.0.is_empty() {
            Err("username must not be empty")
        } else {
            self.0
                .as_str()
                .try_to_inline()
                .map_err(|_| "username too long or contains NULs")
        }
    }
}

impl TryFromInline<'_, ShortString> for Username {
    type Error = &'static str;

    fn try_from_inline(value: &Inline<ShortString>) -> Result<Self, Self::Error> {
        String::try_from_inline(value)
            .map(Username)
            .map_err(|_| "invalid utf-8 or too long")
    }
}
```

### Schema identifiers

Every schema declares a unique 128‑bit identifier, accessible via the
`MetaDescribe::id` method (for example, `ShortString::id()`).
Persisting these IDs keeps serialized data self describing so other tooling can
make sense of the payload without linking against your Rust types. Dynamic
language bindings (like the Python crate) inspect the stored schema identifier
to choose the correct decoder, while internal metadata stored inside Trible
Space can use the same IDs to describe which schema governs a value, blob, or
hash protocol.

Identifiers also make it possible to derive deterministic attribute IDs when you
ingest external formats. Wrap the source field name in an entity-core fragment —
`Attribute::<S>::from(entity!{ metadata::name: <name handle>, metadata::value_encoding: <S as MetaDescribe>::id() })` —
to combine the schema ID with the source field name and produce a stable
attribute so re-importing the same data always targets the same column.
The `attributes!` macro applies the same derivation when you omit the 128-bit id
literal, which is useful for quick experiments or internal attributes; for
schema that will be shared across binaries or languages prefer explicit ids so
the column remains stable even if the attribute name later changes.

## Built‑in inline encodings

The crate provides the following inline encodings out of the box:
- `GenId` &ndash; an abstract 128 bit identifier.
- `ShortString` &ndash; a UTF-8 string up to 32 bytes.
- `U256BE` / `U256LE` &ndash; 256-bit unsigned integers.
- `I256BE` / `I256LE` &ndash; 256-bit signed integers.
- `R256BE` / `R256LE` &ndash; 256-bit rational numbers.
- `F64` &ndash; IEEE-754 double-precision floating point number (little-endian).
- `F256BE` / `F256LE` &ndash; 256-bit floating point numbers.
- `Hash` and `Handle` &ndash; cryptographic digests and blob handles (see [`hash.rs`](../src/value/schemas/hash.rs)).
- `ED25519RComponent`, `ED25519SComponent` and `ED25519PublicKey` &ndash; signature fields and keys.
- `NsTAIInterval` to encode time intervals.
- `Boolean` &ndash; all-zero for false, all-0xFF for true.
- `LineLocation` &ndash; a `(start_line, start_col, end_line, end_col)` span encoded as four big-endian u64 values.
- `RangeU128` &ndash; a half-open `(start, end)` range of two big-endian u128 values.
- `RangeInclusiveU128` &ndash; an inclusive `(start, end)` range of two big-endian u128 values.
- `UnknownInline` as a fallback when no specific schema is known.

```rust
# use triblespace::prelude::*;
use triblespace::core::metadata::MetaDescribe;
use triblespace::core::inline::encodings::shortstring::ShortString;
use triblespace::core::inline::{IntoInline, InlineEncoding};

let v: Inline<ShortString> = "hi".to_inline();
let raw_bytes = v.raw; // Persist alongside the schema's metadata id.
let schema_id = ShortString::id(); // derived via describe(&mut scratch).root()
```

## Built‑in blob encodings

The crate also ships with these blob encodings:

- `LongString` for arbitrarily long UTF‑8 strings.
- `RawBytes` for opaque file-backed byte payloads.
- `SimpleArchive` which stores a raw sequence of tribles.
- `SuccinctArchiveBlob` which stores the [`SuccinctArchive` index
  type](https://docs.rs/triblespace/latest/triblespace/core/blob/schemas/succinctarchive/struct.SuccinctArchive.html)
  for offline queries. The `SuccinctArchive` helper exposes high-level
  iterators while the `SuccinctArchiveBlob` schema is responsible for the
  serialized byte layout.
- `WasmCode` for WebAssembly bytecode stored as a blob.
- `UnknownBlob` for data of unknown type.

```rust
use triblespace::core::metadata::MetaDescribe;
use triblespace::core::blob::encodings::longstring::LongString;
use triblespace::core::blob::{Blob, BlobEncoding, IntoBlob};

let b: Blob<LongString> = "example".to_blob();
let schema_id = LongString::id(); // derived via describe(&mut scratch).root()
```

Both value and blob encodings can emit optional discovery metadata. Calling
`MetaDescribe::describe` returns a rooted `Fragment` (exporting the schema id)
whose facts tag the schema entity with `metadata::KIND_INLINE_ENCODING` or
`metadata::KIND_BLOB_ENCODING` and may attach a `metadata::name` and
`metadata::description` (LongString handles). Persist the description blobs
alongside the metadata tribles if you want the text to remain readable.

## Choosing the right schema

When defining an attribute, the schema determines how the 32-byte value slot is
interpreted. Use this decision tree to pick the right one:

```text
What are you storing?
│
├─ A reference to another entity?
│  └─ GenId
│
├─ A tag, category, or enum-like classifier?
│  └─ metadata::tag (GenId) — tags are entities with their own ID.
│     Use metadata::name to give them a human-readable label.
│     ⚠ Do NOT define a separate ShortString tag attribute —
│     use the canonical metadata::tag and mint tag IDs.
│
├─ A short label or display name?
│  ├─ Fits in 32 bytes (≤32 UTF-8 bytes)?
│  │  └─ ShortString
│  └─ Longer text?
│     └─ Handle<LongString>  (blob)
│
├─ A number?
│  ├─ Integer
│  │  ├─ Fits in 64 bits? → U256BE (zero-extended) or custom u64 schema
│  │  └─ Needs full 256 bits? → U256BE / I256BE
│  ├─ Floating point
│  │  ├─ Standard double? → F64
│  │  └─ Extended precision? → F256BE
│  └─ Rational? → R256
│
├─ A timestamp or time range?
│  └─ NsTAIInterval
│
├─ A cryptographic value?
│  ├─ Content hash? → Hash<Blake3>
│  ├─ Reference to a blob? → Handle<BlobEncoding>
│  └─ Signature? → ED25519RComponent / ED25519SComponent / ED25519PublicKey
│
├─ A file or binary payload?
│  └─ Handle<RawBytes>  (blob)
│
├─ A large structured dataset?
│  └─ Handle<SimpleArchive>  (blob, stores a TribleSet)
│
└─ Something else?
   ├─ Fits in 32 bytes? → define a custom InlineEncoding
   └─ Larger? → define a custom BlobEncoding + use Handle
```

**Rules of thumb:**
- If two values should be joinable (appear in the same query variable), they must
  share a schema. Choose the most specific schema that covers both uses.
- Prefer `ShortString` over `LongString` when the text fits — inline values avoid
  a blob lookup.
- Use `GenId` for relationships between entities. Never store entity references as
  strings.
- When in doubt between a inline encoding and a blob, ask: "will I ever want to
  query or join on this directly?" If yes, it should be a value. If it's opaque
  content you just retrieve, use a blob handle.

## Defining new schemas

Custom formats implement [`InlineEncoding`] or [`BlobEncoding`].  A unique identifier
serves as the schema ID.  The example below defines a little-endian `u64` value
schema and a simple blob encoding for arbitrary bytes.

```rust,ignore
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
   `UnknownInline`/`UnknownBlob` allow you to safely defer decoding until a newer
   binary is available.
3. **Keep validation centralized.** Place invariants in your schema
   conversions so migrations cannot accidentally create invalid values.

By keeping schema identifiers alongside stored values and blobs you can roll out
new representations incrementally: ship readers that understand both IDs, update
your import pipelines, and finally switch writers once everything recognizes the
replacement schema.

## Inline formatters (WASM)

Binary formats are great for portability and performance, but they can be
painful to inspect if you don’t know the schema ahead of time. TribleSpace
supports an optional schema-level formatter mechanism: a inline encoding can point
to a small sandboxed WebAssembly module that turns its raw 32 bytes into a
human-readable string.

The formatter is stored as a blob (`blobencodings::WasmCode`) and referenced from
the schema identifier entity via the metadata attribute `metadata::value_formatter`.

The built-in runner lives behind the `wasm` feature flag (enabled by default in
the `triblespace` facade crate) and uses `wasmi` with tight limits (fuel, memory
pages, output size). Modules must not import anything and use the following
minimal ABI:

- `memory` (linear memory)
- `format(w0: i64, w1: i64, w2: i64, w3: i64) -> i64`

The `format` arguments are the raw 32 bytes split into 4×8-byte chunks
(little-endian). The return value packs the output pointer and output length:

- Success returns `(output_len << 32) | output_ptr` with `output_ptr != 0`.
- Failure returns `(error_code << 32) | 0` (i.e. `output_ptr == 0`).

The core crate can optionally ship built-in formatters for its built-in value
schemas. Enable the `wasm` feature to have
`MetaDescribe::describe` (which is fallible) attach `metadata::value_formatter` entries for the
standard schemas. This feature requires the `wasm32-unknown-unknown` Rust
target at build time because the bundled formatters are compiled to WebAssembly
via the `#[value_formatter]` proc macro.
