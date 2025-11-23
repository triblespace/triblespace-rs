# Pile Format

The on-disk pile keeps every blob and branch in one append-only file. This layout provides a simple **write-ahead log** style database where new data is only appended. It allows both blob and branch storage in a single file while remaining resilient to crashes. The pile backs local repositories and acts as a durable content‑addressed store. The pile file can be memory mapped for fast reads and is safely shared between threads because existing bytes are never mutated.

While large databases often avoid `mmap` due to pitfalls with partial writes
and page cache thrashing [[1](https://db.cs.cmu.edu/mmap-cidr2022/)], this
design works well for the pile's narrow usage pattern.

## Design Rationale

This format emphasizes **simplicity** over sophisticated on-disk structures.
Appending new blobs rather than rewriting existing data keeps corruption
windows small and avoids complicated page management. Storing everything in a
single file makes a pile easy to back up or replicate over simple transports
while still allowing it to be memory mapped for fast reads. The 64&nbsp;byte
alignment ensures each entry begins on a cache line boundary, which improves
concurrent access patterns and allows safe typed views with the `zerocopy`
crate.

## Immutability Assumptions

A pile is treated as an immutable append-only log. Once a record sits below a
process's applied offset, its bytes are assumed permanent. The implementation
does not guard against mutations; modifying existing bytes is undefined
behavior. Only the tail beyond the applied offset might hide a partial append
after a crash, so validation and repair only operate on that region. Each
record's validation state is cached for the lifetime of the process under this
assumption.

Hash verification only happens when blobs are read. Opening even a very large
pile is therefore fast while still catching corruption before data is used.

Every record begins with a 16&nbsp;byte magic marker that identifies whether it
stores a blob or a branch. The sections below illustrate the layout of each
type.

## Usage

A pile typically lives as a `.pile` file on disk. Repositories open it through
`Pile::open` and then call [`refresh`](../../src/repo/pile.rs) to load existing
records or [`restore`](../../src/repo/pile.rs) to repair after a crash. Multiple
threads may share the same handle thanks to internal synchronisation, making a
pile a convenient durable store for local development. Blob appends use a single
`O_APPEND` write. Each handle remembers the last offset it processed and, after
appending, scans any gap left by concurrent writes before advancing this
`applied_length`. Writers may race and duplicate blobs, but content addressing
keeps the data consistent. Each handle tracks hashes of pending appends
separately so repeated writes are deduplicated until a `refresh`. A `restore`
clears this in-memory set to discard truncated appends. Branch updates only record
the referenced hash and do not verify that the corresponding blob exists in the
pile, so a pile may act as a head-only store when blob data resides elsewhere.
Updating branch heads requires a brief critical
section: `flush → refresh → lock → refresh → append → unlock`. The initial
`refresh` acquires a shared lock so it cannot race with `restore`, which takes an
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
Each blob entry records its creation timestamp, the length of the payload (which may be zero) and
its hash. The payload is padded so the next record begins on a
64&nbsp;byte boundary. The [Pile Blob Metadata](./pile-blob-metadata.md) chapter explains how to
query these fields through the `PileReader` API.

## Branch Storage
```
            ┌────16 byte───┐┌────16 byte───┐┌────────────32 byte───────────┐
          ┌ ┌──────────────┐┌──────────────┐┌──────────────────────────────┐
 header   │ │magic number B││  branch id   ││             hash             │
          └ └──────────────┘└──────────────┘└──────────────────────────────┘
```
Branch entries map a branch identifier to the hash of a blob.
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
documentation](https://docs.rs/triblespace/latest/triblespace/repo/pile/struct.Pile.html).

[1]: https://db.cs.cmu.edu/mmap-cidr2022/ "The Case Against Memory-Mapped I/O"
