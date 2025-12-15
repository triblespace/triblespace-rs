# Pile Blob Metadata

Every blob stored in a pile begins with a header that records the moment it was
written and the length of the payload that follows. The `Pile` implementation
surfaces that information so tools can answer questions such as "when did this
blob arrive?" without re-parsing the file on disk.

## `BlobMetadata`

Blob metadata is exposed through the [`BlobMetadata`][blobmetadata] struct. It is
a simple value type containing two public fields:

- `timestamp`: the write time stored in the blob header. The pile records the
  number of **milliseconds since the Unix epoch** when the blob was appended.
- `length`: the size of the blob payload in bytes. Padding that aligns entries to
  64&nbsp;byte boundaries is excluded from this value, so it matches the slice
  returned by [`PileReader::get`][get].

[blobmetadata]: ../../src/repo/pile.rs
[get]: ../../src/repo/pile.rs

## Looking up blob metadata

`PileReader::metadata` accepts the same `Value<Handle<_, _>>` that other blob
store APIs use. It returns `Some(BlobMetadata)` when the blob exists and the
stored bytes hash to the expected value; otherwise it yields `None`.

Readers operate on the snapshot that was current when they were created. Call
[`Pile::refresh`][refresh] and request a new reader to observe blobs appended
afterwards.

[refresh]: ../../src/repo/pile.rs

```rust,no_run
use anybytes::Bytes;
use triblespace::blob::schemas::UnknownBlob;
use triblespace::blob::Blob;
use triblespace::repo::pile::Pile;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pile = Pile::open("/tmp/example.pile")?;

    let blob = Blob::<UnknownBlob>::new(Bytes::from_static(b"hello world"));
    let handle = pile.put(blob)?;

    let reader = pile.reader()?;
    if let Some(meta) = reader.metadata(handle) {
        println!(
            "Blob length: {} bytes, appended at {} ms",
            meta.length, meta.timestamp
        );
    }

    Ok(())
}
```

## Failure cases

`metadata` returns `None` in a few situations:

- the handle does not correspond to any blob stored in the pile;
- the reader snapshot predates the blob (refresh the pile and create a new
  reader to see later writes);
- validation previously failed because the on-disk bytes did not match the
  recorded hash, for example after the pile file was corrupted before this
  process opened it.

When `None` is returned, callers can treat it the same way they would handle a
missing blob from `get`: the data is considered absent from the snapshot they are
reading.

For more detail on how the metadata is laid out on disk, see the
[Pile Format](./pile-format.md) chapter.
