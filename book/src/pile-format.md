# Pile Format

The on-disk pile keeps every blob and branch in one append-only file. The
write-ahead log *is* the database: all indices are reconstructed from the bytes
already stored on disk. This design avoids background compaction, manifest
management, or auxiliary metadata while still providing a durable
content-addressed store for local repositories. The pile file is memory mapped
for fast, zero-copy reads and can be safely shared between threads because
existing bytes are never mutated—once data is validated it remains stable.

While large databases often avoid `mmap` due to pitfalls with partial writes and
page cache thrashing [[1](https://db.cs.cmu.edu/mmap-cidr2022/)], the pile's
narrow usage pattern keeps these failure modes manageable. Appends happen
sequentially and validation walks new bytes before readers observe them, so the
memory map never exposes half-written records.

## Design Rationale

This format emphasizes **simplicity** over sophisticated on-disk structures.
Appending new blobs rather than rewriting existing data keeps corruption
windows small and avoids complicated page management. Storing everything in a
single file makes a pile easy to back up or replicate over simple transports
while still allowing it to be memory mapped for fast reads. The 64&nbsp;byte
alignment ensures each entry begins on a cache line boundary, improves
concurrent access patterns, and allows safe typed views with the `zerocopy`
crate. Internally the pile tracks an `applied_length` watermark; offsets below
this boundary are known-good and only the tail beyond it is rescanned when
refreshing state.

## Operational workflow

1. **Open or create the file.** `Pile::open` builds the struct around a `File`
   handle and `memmap2` mapping.
2. **Repair the tail.** `restore` acquires an exclusive lock, reapplies intact
   records, and truncates any partial append left by a prior crash. Run this
   once after opening so the pile starts from a known-good tail.
3. **Rely on implicit refreshes.** `refresh` acquires a shared lock, walks bytes
   beyond `applied_length`, and rebuilds the blob/branch indices in memory, but
   callers rarely need to invoke it directly. `reader`, `branches`, `head`, and
   `update` call `refresh` internally before they inspect or apply records, so
   external writers are visible without a standalone scan.
4. **Append new records.** `put` (through the `BlobStorePut` trait) and branch
   update helpers extend the file via a single `write_vectored` call. Each
   append immediately feeds the bytes back through `apply_next` so in-memory
   indices stay synchronised without waiting for a manual `refresh`.
5. **Read through a snapshot.** `reader` clones the memory map and PATCH indices
   into a `PileReader`, yielding iterators and metadata lookups that can execute
   without further locking.

This lifecycle keeps pile usage predictable: open → restore → let operations
refresh as they run → mutate → hand out read-only readers. If a process wants to
scan for new appends between operations (for example, a background monitor that
is not issuing `reader` or branch calls), it can explicitly call `refresh` to
pick up external writers without blocking them for long.

## Immutability Assumptions

A pile is treated as an immutable append-only log. Once a record sits below a
process's applied offset, its bytes are assumed permanent. The implementation
does not guard against mutations; modifying existing bytes is undefined
behavior. Only the tail beyond the applied offset might hide a partial append
after a crash, so validation and repair only operate on that region. Each
record's validation state is cached for the lifetime of the process under this
assumption, avoiding repeated hash verification for frequently accessed blobs.

Hash verification only happens when blobs are read. Opening even a very large
pile is therefore fast while still catching corruption before data is used.

Every record begins with a 16&nbsp;byte magic marker that identifies whether it
stores a blob or a branch. The sections below illustrate the layout of each
type.

## Usage

A pile typically lives as a `.pile` file on disk. Repositories open it through
`Pile::open` and then call [`restore`](../../src/repo/pile.rs) to repair any
partial appends left by a prior crash. This rescans the file, truncates the tail
to the last valid record, and clears the in-memory set of pending hashes while
applying every intact record found along the way. A subsequent
[`refresh`](../../src/repo/pile.rs) is only needed when a caller wants to scan
for *new* writes without performing an operation that already refreshes, such as
creating a reader or listing branches. Calling `refresh` immediately after
`restore` is therefore usually a no-op. In single-writer deployments this
"restore-then-operate" startup sequence is typically sufficient because only the
local process can leave a torn tail. Multi-writer systems may still refresh
periodically (and run `restore` when corruption is reported) because other
processes can introduce new bytes—and new errors—after the initial repair.
Multiple threads may share the same handle thanks to internal synchronisation,
making a pile a convenient durable store for local development. Blob appends use
a single `O_APPEND` write. Each handle remembers the last offset it processed
and, after appending, scans any gap left by concurrent writes before advancing
this `applied_length`. Writers may race and duplicate blobs, but content
addressing keeps the data consistent. Each handle tracks hashes of pending
appends separately so repeated writes are deduplicated until a `refresh`.
Branch updates only record the referenced hash and do not verify that the
corresponding blob exists in the pile, so a pile may act as a head-only store
when blob data resides elsewhere.

```rust,no_run
use std::error::Error;
use std::path::PathBuf;

use anybytes::Bytes;
use tribles::prelude::*;
use tribles::repo::pile::ReadError;
use tribles::repo::BlobStoreMeta;

fn add_blob(bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from("data.pile");
    let mut pile = Pile::open(&path)?;
    // Perform one repair scan up front to discard any partially written data
    // left by a prior crash. This truncates the file to the last valid record.
    match pile.restore() {
        Ok(()) => {}
        Err(err @ ReadError::CorruptPile { .. }) => {
            // `restore` already truncated as far as it could; propagate the
            // error so callers can decide whether to retry or abort startup.
            return Err(err.into());
        }
        Err(other) => return Err(other.into()),
    }

    // Insert a blob and obtain a handle pointing at the on-disk bytes.
    let handle = pile.put(Bytes::from_source(bytes.to_vec()))?;

    // Readers operate on a snapshot cloned from the pile's mmap.
    let reader = pile.reader()?;
    if let Some(meta) = reader.metadata(handle)? {
        println!("stored {} bytes at {}", meta.length, meta.timestamp);
    }
    Ok(())
}
```

This pattern illustrates the typical flow: open, repair with `restore`, rely on
the built-in refreshes performed by `reader` and branch helpers, mutate via
`put`, then hand the `PileReader` snapshot to read-only consumers. Updating
branch heads requires a brief critical section—`flush → refresh → lock →
refresh → append → unlock`—so a caller observes a consistent head even when
multiple processes contend for the same file descriptor. The initial `refresh`
acquires a shared lock so it cannot race with `restore`, which takes an
exclusive lock before truncating a corrupted tail.

Filesystems lacking atomic `write`/`vwrite` appends—such as some network or
FUSE-based implementations—cannot safely host multiple writers and are not
supported. Using such filesystems risks pile corruption.
## Blob Storage
```
                             8 byte  8 byte
            ┌────16 byte───┐┌──────┐┌──────┐┌────────────32 byte───────────┐
          ┌ ┌──────────────┐┌──────┐┌──────┐┌──────────────────────────────┐
 header   │ │magic number A││ time ││length││             hash             │
          └ └──────────────┘└──────┘└──────┘└──────────────────────────────┘
            ┌────────────────────────────64 byte───────────────────────────┐
          ┌ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
          │ │                                                              │
 payload  │ │              bytes (64byte aligned and padded)               │
          │ │                                                              │
          └ └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘
```
Each blob entry records:

- **Magic marker** – distinguishes blob records from branches.
- **Timestamp** – milliseconds since the Unix epoch when the append occurred.
- **Payload length** – the unpadded byte length of the blob.
- **Hash** – the digest produced by the pile's hash protocol (BLAKE3 by default) and used as the blob handle.

The payload follows and is padded so the next record begins on a 64&nbsp;byte
boundary. The [Pile Blob Metadata](./pile-blob-metadata.md) chapter explains how
to query these fields through the `PileReader` API.

## Branch Storage
```
            ┌────16 byte───┐┌────16 byte───┐┌────────────32 byte───────────┐
          ┌ ┌──────────────┐┌──────────────┐┌──────────────────────────────┐
 header   │ │magic number B││  branch id   ││             hash             │
          └ └──────────────┘└──────────────┘└──────────────────────────────┘
```
Branch entries map a branch identifier to the hash of a blob. Branch appends are
intentionally lightweight: the pile does not check whether the referenced blob
exists locally, allowing deployments that store heads on disk while serving blob
contents from a remote store.
## Recovery
Calling [`refresh`](../../src/repo/pile.rs) scans an existing file to ensure
every header uses a known marker and that the whole record fits. It does not
verify any hashes. If a truncated or unknown block is found the function reports
the number of bytes that were valid so far using
[`ReadError::CorruptPile`].

If the file shrinks between scans into data that has already been applied, the
process aborts immediately. Previously returned `Bytes` handles would dangle and
continuing could cause undefined behavior, so truncation into validated data is
treated as unrecoverable.

`refresh` holds a shared file lock while scanning. This prevents a concurrent
[`restore`](../../src/repo/pile.rs) call from truncating the file out from under
the reader.

The [`restore`](../../src/repo/pile.rs) helper re-runs the same validation and
truncates the file to the valid length if corruption is encountered. This
recovers from interrupted writes by discarding incomplete data. Hash
verification happens lazily only when individual blobs are loaded so that
opening a large pile remains fast.

For more details on interacting with a pile see the [`Pile` struct
documentation](https://docs.rs/tribles/latest/tribles/repo/pile/struct.Pile.html).

[1]: https://db.cs.cmu.edu/mmap-cidr2022/ "The Case Against Memory-Mapped I/O"
