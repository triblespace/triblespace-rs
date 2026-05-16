# Triblespace Schemas (Quick Notes)

## Built-in blob schemas

- `LongString` — UTF-8 text stored as a blob.
- `RawBytes` — opaque file-backed byte payloads.
- `SimpleArchive` — archived `TribleSet` blobs.
- `SuccinctArchiveBlob` — compressed succinct archive index blobs.
- `WasmCode` — WebAssembly modules stored as blobs.
- `UnknownBlob` — fallback when the blob schema is not known.

For details, see `triblespace-rs/book/src/schemas.md`.

## Metadata usage annotations

- `metadata::attribute` — links a usage annotation entity to the attribute id it describes.
- `metadata::source` — optional free-form provenance string for the usage.
- `metadata::source_module` — optional module path for the usage.
- `metadata::name` / `metadata::description` are used on the usage entity
  to record contextual names and docs without forcing a single canonical label on the attribute id.

Importers use a private attribute wrapper that emits `metadata::name` and
`metadata::value_encoding` directly on the attribute id, but *does not* emit usage
annotations. Usage annotations are reserved for code-defined attributes where
the source context matters.
