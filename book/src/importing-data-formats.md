# Importing Other Data Formats

Import pipelines let you bring external datasets into a tribles repository without
hand-writing schemas or entity identifiers every time. This chapter introduces the
`import` namespace, explains how the JSON importers map foreign fields onto
attributes, and outlines how you can extend the same patterns to new formats.

## Import Namespace Overview

The `triblespace_core::import` module collects conversion helpers that translate
structured documents into raw tribles. Today the namespace ships with two JSON
importers:

- `JsonImporter` generates fresh entity identifiers for every object it visits.
- `DeterministicJsonImporter` derives entity identifiers by hashing the encoded
  attribute/value pairs so the same input always reproduces the same entities.

Both variants accept encoder callbacks for JSON primitives. Those closures turn
strings, numbers, and booleans into `Value` instances and can allocate blobs or
perform validation before handing the data back to the importer. The
`valueschemas::Boolean` helper stores `false` as all-zero bytes and `true` as
all ones so JSON flags round-trip without ambiguity when you wire the boolean
encoder to it.

Both importers accumulate statements internally. After feeding one or more
JSON documents through `import_value` or `import_str`, call `data()` to inspect
the tribles that were emitted and `metadata()` to retrieve attribute
descriptors. When you want to start a fresh batch without recomputing attribute
hashes, `clear_data()` drops the accumulated statements while leaving the
attribute caches intact. Call `clear()` to reset both the staged data and the
attribute caches when you need a completely fresh run.

## Mapping JSON Fields to Attributes

Attributes are derived through `Attribute::from_name`, which hashes the JSON
field name together with the `ValueSchema` selected for that primitive. The
importers cache the resulting `RawId`s per field and schema so the hash only has
to be computed once per run. Arrays are treated as multi-valued fields: every
item is encoded and stored under the same attribute identifier, producing one
trible per element.

After an import completes the importers regenerate metadata from their cached
attribute maps. The [`metadata()`](crate::import::json::JsonImporter::metadata)
accessor returns tribles that link each derived attribute id to its field name,
value schema, and optional blob schema. Merge those descriptors into your
repository alongside the imported data when you want queries to discover the
original JSON field names or project datasets by schema without repeating the
derivation logic.

Nested objects recurse automatically. The parent receives a `GenId` attribute
that points at the child entity, allowing the importer to represent the entire
object graph as a connected set of tribles. Because those `GenId` attributes are
also derived from the parent field names they remain stable even when you import
related documents in separate batches.

## Managing Entity Identifiers

`JsonImporter::new` defaults to `ufoid()` for identifier generation, but the
constructor is parameterized so you can inject your own policy—for example, a
`fucid()` generator or any other closure that returns an `ExclusiveId`. The
custom generator is applied consistently to every object the importer touches,
including nested documents.

`DeterministicJsonImporter` takes a different approach. It buffers the encoded
attribute/value pairs for each object, sorts them, and feeds the resulting byte
stream into a user-supplied hash protocol. The first 16 bytes of that digest
become the entity identifier, ensuring identical JSON inputs produce identical
IDs even across separate runs. Once the identifier is established, the importer
writes the cached pairs into its internal trible set via `Trible::new`, so
subsequent calls to `data()` expose the deterministic statements alongside the
metadata generated for every derived attribute.

This hashing step also changes how repeated structures behave. When a JSON
document contains identical nested objects—common in fixtures such as
`citm_catalog` or Twitter exports—the deterministic importer emits the same
identifier for each recurrence. Only the first copy reaches the underlying
`TribleSet`; later occurrences are recognised as duplicates and skipped during
the merge. The nondeterministic importer must still mint a fresh identifier for
every repetition, so it inserts and deduplicates a full set of triples each
time. Even if the ID generator itself is fast, that extra merge work makes the
deterministic importer benchmark faster on datasets with significant repetition.

## Working with Encoder Callbacks

Encoder callbacks receive borrowed references to the raw JSON values. Because the
closures are generic over a lifetime you can capture external resources—like a
blob store connection or a schema registry—without allocating reference-counted
wrappers. Callers can stage binary payloads in whichever blob backend they
prefer and return handles that will be persisted alongside the tribles.

The callbacks report failures through `EncodeError`. You can construct an error
with a simple message or wrap an existing error type. The importer surfaces the
field name alongside the original error so schema mismatches remain easy to
diagnose while keeping the hot path lightweight.

## Extending the Importers

To support a new external format, implement a module in the `import` namespace
that follows the same pattern: decode the source data, derive attributes with
`Attribute::from_name`, and hand encoded values to `Trible::new`. Reuse the
lifetime-parameterized encoder callbacks so callers can plug in existing blob
stores or validation logic. If the format supplies stable identifiers, offer a
constructor that accepts a custom generator or hash protocol so downstream
systems can choose between ephemeral and deterministic imports.

