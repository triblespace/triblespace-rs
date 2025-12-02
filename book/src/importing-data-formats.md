# Importing Other Data Formats

Import pipelines let you bring external datasets into a tribles repository without
hand-writing schemas or entity identifiers every time. This chapter introduces the
`import` namespace, explains how the JSON importers map foreign fields onto
attributes, and outlines how you can extend the same patterns to new formats.

## Import Namespace Overview

The `triblespace_core::import` module collects conversion helpers that translate
structured documents into raw tribles. Today the namespace ships with a single
deterministic JSON importer:

- `JsonImporter` hashes attribute/value pairs to derive entity identifiers so
  identical inputs reproduce the same entities. Construct it with a blob sink
  (e.g., a `Workspace`’s store or a `MemoryBlobStore`) and an optional 32-byte
  salt when you want to mix in extra entropy to avoid collisions.

The importer uses a fixed mapping for JSON primitives:

- strings → `Handle<Blake3, LongString>`
- numbers → `F256`
- booleans → `Boolean`

Arrays are treated as multi-valued fields; every element becomes its own trible
under the same attribute identifier. Nested objects recurse automatically,
linking parent to child entities through `GenId` attributes derived from the
containing field name. After feeding one or more JSON documents through
`import_value` or `import_str`, call `data()` to inspect the emitted tribles and
`metadata()` to retrieve attribute descriptors and multi-value hints (a
`metadata::tag` edge pointing to `metadata::KIND_MULTI`). Use
`clear_data()` to drop accumulated statements while keeping attribute caches, or
`clear()` when you need a completely fresh run.

## Mapping JSON Fields to Attributes

Attributes are derived through `Attribute::from_name`, which hashes the JSON
field name together with the fixed `ValueSchema` for that primitive. The
importer caches the resulting `RawId`s per field so the hash only has to be
computed once per run. Arrays are treated as multi-valued fields: every item is
encoded and stored under the same attribute identifier, producing one trible per
element.

After an import completes the importer regenerates metadata from its cached
attribute map. Call `data()` to inspect the emitted tribles and `metadata()` to
retrieve attribute descriptors and multi-value hints (via `metadata::tag`
pointing to `metadata::KIND_MULTI`). Merge those descriptors into your
repository alongside the imported data when you want queries to discover the
original JSON field names or project datasets by schema without repeating the
derivation logic.

When exporting back to JSON, pass a blob reader (e.g., from a `Workspace` or
`MemoryBlobStore`) to `export_to_json` so longstrings can be inlined. If a blob
is missing or unreadable the exporter returns an error with the handle hash
instead of silently emitting a placeholder, keeping roundtrips lossless when
blobs are present. The exporter uses the same fixed mapping in reverse:
`ShortString` → JSON string, `Handle<Blake3, LongString>` → JSON string (via
blob lookup), `Boolean` → JSON bool, `F256` → JSON number, `GenId` → inlined
object (unless already visited). Attributes that use other schemas are ignored
so JSON roundtrips stay predictable even when the dataset mixes in
format-specific extensions.

Nested objects recurse automatically. The parent receives a `GenId` attribute
that points at the child entity, allowing the importer to represent the entire
object graph as a connected set of tribles. Because those `GenId` attributes are
also derived from the parent field names they remain stable even when you import
related documents in separate batches.

## Managing Entity Identifiers

The importer buffers the encoded attribute/value pairs for each object, sorts
them, and feeds the resulting byte stream into a hash protocol. The first 16
bytes of that digest become the entity identifier, ensuring identical JSON
inputs produce identical IDs even across separate runs. You can supply an
optional 32-byte salt via the constructor to keep deterministic imports from
colliding with existing data. Once the identifier is established,
the importer writes the cached pairs into its trible set via `Trible::new`, and
exposes the data, metadata, and root entity identifiers for each imported
document through its accessors.

This hashing step also changes how repeated structures behave. When a JSON
document contains identical nested objects—common in fixtures such as
`citm_catalog` or Twitter exports—the deterministic importer emits the same
identifier for each recurrence. Only the first copy reaches the underlying
`TribleSet`; later occurrences are recognised as duplicates and skipped during
the merge. Even if the hash itself is fast, that deduplication step reduces
workload on datasets with significant repetition.

## Extending the Importers

To support a new external format, implement a module in the `import` namespace
that follows the same pattern: decode the source data, derive attributes with
`Attribute::from_name`, encode values using the appropriate `ValueSchema`, and
hand the results to `Trible::new`. If the format supplies stable identifiers,
mix them into the hashing step or salt so downstream systems can keep imports
idempotent.
