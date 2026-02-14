# Importing Other Data Formats

Import pipelines let you bring external datasets into a tribles repository without
hand-writing schemas or entity identifiers every time. This chapter introduces the
`import` namespace, explains how the JSON importers map foreign fields onto
attributes, and outlines how you can extend the same patterns to new formats.

## Import Namespace Overview

The `triblespace_core::import` module collects conversion helpers that translate
structured documents into raw tribles. Today the namespace ships with two
deterministic JSON importers:

- `JsonObjectImporter` hashes attribute/value pairs to derive entity identifiers
  so identical inputs reproduce the same entities. It accepts a top-level JSON
  object (or a top-level array of objects). Construct it with a blob sink (e.g.,
  a `Workspace`’s store or a `MemoryBlobStore`) and an optional 32-byte salt when
  you want to mix in extra entropy to avoid collisions. Each `import_*` call
  returns a [`Fragment`](../src/trible/fragment.rs) that exports the root entity
  id(s) and contains the emitted facts.
- `JsonTreeImporter` preserves the full JSON structure and ordering by emitting
  explicit node and entry entities (a JSON AST). It derives content-addressed
  identifiers from the JSON values themselves so identical subtrees deduplicate
  across overlapping imports. Unlike the object importer it can represent
  arbitrary JSON roots, including primitives. Each `import_*` call returns a
  rooted `Fragment` for the imported JSON value.

`JsonObjectImporter` uses a fixed mapping for JSON primitives:

- strings → `Handle<Blake3, LongString>`
- numbers → `F64`
- booleans → `Boolean`

Arrays are treated as multi-valued fields; every element becomes its own trible
under the same attribute identifier. Nested objects recurse automatically,
linking parent to child entities through `GenId` attributes derived from the
containing field name. After one or more imports, call `metadata()` to retrieve
attribute descriptors and multi-value hints (a `metadata::tag` edge pointing to
`metadata::KIND_MULTI`). Use `clear()` when you need a completely fresh run
(drop the per-field attribute caches and multi-value tracking).

## Mapping JSON Fields to Attributes

Attributes are derived through `Attribute::from_name`, which hashes the JSON
field name together with the fixed `ValueSchema` for that primitive. The
importer caches the resulting `RawId`s per field so the hash only has to be
computed once per run. Arrays are treated as multi-valued fields: every item is
encoded and stored under the same attribute identifier, producing one trible per
element.

After an import completes the importer regenerates metadata from its cached
attribute map. The returned `Fragment` contains the emitted facts; call
`metadata()` to retrieve attribute descriptors and multi-value hints (via
`metadata::tag` pointing to `metadata::KIND_MULTI`). Merge those descriptors
into your repository alongside the imported facts when you want queries to
discover the original JSON field names or project datasets by schema without
repeating the derivation logic. Field names are stored as `metadata::name`
handles to
LongString blobs so arbitrarily long keys survive roundtrips; `metadata::name`
is a general-purpose entity naming attribute, but importers use it for field
names here. Importers intentionally avoid emitting attribute *usage* annotations;
those are reserved for code-defined attributes so each codebase can attach its
own contextual names and descriptions.

You can import multiple documents by merging fragments:

```rust,ignore
let mut all = Fragment::default();
all += importer.import_str(doc1)?;
all += importer.import_str(doc2)?;
// all.exports() yields the root ids; all.facts() yields the merged tribles.
```

When exporting back to JSON, pass a blob reader (e.g., from a `Workspace` or
`MemoryBlobStore`) to `export_to_json` so longstrings can be inlined. If a blob
is missing or unreadable the exporter returns an error with the handle hash
instead of silently emitting a placeholder, keeping roundtrips lossless when
blobs are present. The exporter uses the same fixed mapping in reverse:
`ShortString` → JSON string, `Handle<Blake3, LongString>` → JSON string (via
blob lookup), `Boolean` → JSON bool, `F64` → JSON number, `GenId` → inlined
object (unless already visited). Attributes that use other schemas are ignored
so JSON roundtrips stay predictable even when the dataset mixes in
format-specific extensions.

Nested objects recurse automatically. The parent receives a `GenId` attribute
that points at the child entity, allowing the importer to represent the entire
object graph as a connected set of tribles. Because those `GenId` attributes are
also derived from the parent field names they remain stable even when you import
related documents in separate batches.

## Lossless JSON Import

`JsonTreeImporter` trades the compact attribute/value encoding for a
lossless JSON AST representation. Each JSON value becomes a node tagged with a
kind (`json_tree::kind_*`). Objects and arrays emit explicit entry entities
that store field names and indices (`json_tree::field_*` and
`json_tree::array_*`), preserving ordering and allowing repeated keys.
Numbers are stored as raw decimal strings via `Handle<Blake3, LongString>` so
precision is not lost. Array and field indices are stored as `U256BE` to keep
ordering exact even for large collections.

Because node identifiers are derived from the content of each value, identical
subtrees converge automatically when you import overlapping backups. This makes
lossless imports a good archival layer: you can keep full-fidelity raw JSON and
still layer semantic projections on top.

Each `import_*` call returns a rooted `Fragment` containing the JSON AST facts.
Merge fragments when you ingest multiple documents. `metadata()` returns a
fixed set of schema descriptors for the `json_tree::*` attributes and kinds; you
typically merge it once alongside your lossless archive.

## Managing Entity Identifiers

The importer buffers the encoded attribute/value pairs for each object, sorts
them, and feeds the resulting byte stream into a hash protocol. The first 16
bytes of that digest become the entity identifier, ensuring identical JSON
inputs produce identical IDs even across separate runs. You can supply an
optional 32-byte salt via the constructor to keep deterministic imports from
colliding with existing data. Once the identifier is established,
the importer writes the derived pairs into a `TribleSet` via `Trible::new` and
returns them as a `Fragment` whose exports are the root entity id(s) for the
imported document.

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
