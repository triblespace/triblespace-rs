# Pile Blob Metadata

Every blob stored in a pile begins with a compact header. Besides the payload
hash (covered in [Pile Format](./pile-format.md)), the header records *when* the
blob was appended and *how long* the payload is. The `Pile` implementation
surfaces this information so tooling can answer questions such as "when did this
blob arrive?" without walking the raw bytes on disk.

## Header fields at a glance

The header written ahead of every blob contains four fields (64 bytes total):

| Field         | Size (bytes) | Purpose                                                                 |
| ------------- | ------------ | ------------------------------------------------------------------------ |
| Magic marker  | 16           | Distinguishes blob records from branch updates.                         |
| Timestamp     | 8            | Milliseconds since the Unix epoch when the payload was appended.        |
| Length        | 8            | Size of the payload in bytes (padding is stored separately).            |
| Hash          | 32           | The 256-bit digest of the payload used to validate the stored bytes.    |

[`BlobMetadata`][blobmetadata] re-exposes the timestamp and length fields so
callers can read when a blob was appended and how large the payload is.

## `BlobMetadata`

[`BlobMetadata`][blobmetadata] is a lightweight struct shared by all repository
implementations. It mirrors the timestamp/length pair in the header and leaves
validation to the reader:

- `timestamp`: the write time stored in the blob header as a `u64`. A convenient
  way to turn this into a `SystemTime` is shown below. `Pile::put` records this
  value using `SystemTime::now()`, so it reflects wall-clock time and can move
  forward or backward if the system clock is adjusted.
- `length`: the size of the blob payload in bytes. Padding that aligns entries
  to 64-byte boundaries is excluded from this value, so it matches the slice
  returned by [`PileReader::get`][get].

[blobmetadata]: ../../src/repo.rs
[get]: ../../src/repo/pile.rs

## Looking up blob metadata

`PileReader::metadata` accepts the same `Value<Handle<_, _>>` that other blob
store APIs use. The reader consults its in-memory index and, on the first
request for a handle, lazily hashes the payload to confirm the bytes match the
handle. Subsequent metadata lookups for the same handle reuse that cached
validation result. When the payload passes validation the method returns
`Some(BlobMetadata)`; otherwise it yields `None`.

Readers operate on the snapshot that was current when they were created. Call
[`Pile::refresh`][refresh] and request a new reader to observe blobs appended
afterwards. `PileReader::metadata` never fails for valid snapshots—its error
type is [`Infallible`](core::convert::Infallible).

[refresh]: ../../src/repo/pile.rs

```rust,no_run
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anybytes::Bytes;
use tribles::blob::schemas::UnknownBlob;
use tribles::blob::Blob;
use tribles::repo::pile::Pile;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pile = Pile::open("/tmp/example.pile")?;

    let blob = Blob::<UnknownBlob>::new(Bytes::from_static(b"hello world"));
    let handle = pile.put(blob)?;

    let reader = pile.reader()?;
    if let Some(meta) = reader.metadata(handle) {
        let appended_at = UNIX_EPOCH + Duration::from_millis(meta.timestamp);
        println!(
            "Blob length: {} bytes, appended at {:?}",
            meta.length, appended_at
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
missing blob from `get`: the data is considered absent from the snapshot they
are reading. Because validation is cached, later calls will continue to report
`None` for the same handle until a future refresh revalidates the blob.

For additional background on the binary layout and how the header interacts
with padding, see the [Pile Format](./pile-format.md) chapter.
