# Portability & Common Formats

Tribles aims to be a durable, language-agnostic database. That means the bytes we
store must remain meaningful when they cross process, platform, or version
boundaries. Rust's native representations are not stable enough for that job, so
we model portable schemas that describe how a value should be encoded into a
32-byte buffer.

## Why 32 Bytes?

The 32-byte window is a deliberate compromise. It is small enough to move
quickly through memory and across networks, yet it offers enough entropy to hold
intrinsic identifiers such as hashes. When a payload cannot fit within those 32
bytes we store the larger content in a blob and reference it from the value via
its identifier. The uniform size also keeps the storage layer simple: a `Value`
is always the same size regardless of its schema.

## Schemas as Contracts

A schema describes which bit patterns are meaningful for a particular value. The
[`Value`](../../src/value.rs) type is parameterised by such a schema and remains
agnostic about whether the underlying bytes currently satisfy the contract.
Validation lives in the schema through the [`ValueSchema`](../../src/value.rs)
trait, while conversions to concrete Rust types use [`ToValue`], [`FromValue`],
[`TryToValue`], and [`TryFromValue`]. Because conversion traits are implemented
for the schema instead of the `Value` type itself, we avoid Rust's orphan rule
and allow downstream crates to add their own adapters.

Schemas carry two optional identifiers:

- **Value schema ID** – Uniquely distinguishes the schema that governs the 32-byte value buffer.
- **Blob schema ID** – Identifies the schema of any external blob a value may reference.

These identifiers let us document schemas inside the knowledge graph itself.
They also provide stable lookup keys for registries or tooling that need to
understand how bytes should be interpreted.

## Working Safely with Values

Serialisation is effectively a controlled form of transmutation: we reinterpret
bytes through the lens of a schema. Crossing from one schema to another will not
cause undefined behaviour, but it may produce nonsensical data if the target
schema expects a different layout. Validation routines and conversion helpers
exist to guard against that outcome.

When you define a new schema:

1. Create a zero-sized marker type and implement [`ValueSchema`] for it.
2. Add conversions between the schema and your Rust types via the conversion
   traits mentioned above.
3. Validate inputs when only some bit patterns are acceptable.

With those pieces in place, values can round-trip between storage and strongly
typed Rust code while remaining portable and future-proof.

[`ValueSchema`]: ../../src/value.rs
[`ToValue`]: ../../src/value.rs
[`FromValue`]: ../../src/value.rs
[`TryToValue`]: ../../src/value.rs
[`TryFromValue`]: ../../src/value.rs
