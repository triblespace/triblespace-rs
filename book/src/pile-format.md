# Pile Format

The on-disk pile keeps every blob and pin in one append-only file. The
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

## Record model: uniform 256-byte records (V3)

Every record the pile writes today — blob, branch (pin) head, branch
tombstone, weak-pin marker, weak-unpin marker — uses the **V3** layout: a
fixed **256-byte header**, followed (for blobs) by the payload, padded so the
whole record is a **256-byte multiple**. This uniformity is load-bearing:

- **Position independence.** Blob data starts at the constant
  `record_start + 256`; there is no offset-derived padding. A record means
  the same thing at any offset, so records survive relocation and
  `cat a.pile >> b.pile` is a valid merge of two piles.
- **Alignment for free.** Because every record is a 256-byte multiple, a
  pure-V3 pile stays 256-aligned throughout under the atomic lock-free
  append — every blob's payload lands on a 256-byte boundary, satisfying GPU
  storage-buffer binding requirements (CUDA / Metal
  `min_storage_buffer_offset_alignment`) for zero-copy aliasing.
- **Cache-friendly headers.** Each header begins on a cache-line boundary and
  admits safe typed views with the `zerocopy` crate.

Reserved header bytes are zeroed and are **not** part of the content hash;
per-record metadata belongs in tribles, not in the header, so identical bytes
never fork into distinct blobs.

The reader still accepts the original **V1** records (64-byte-aligned blob,
branch, and tombstone layouts — see [Legacy V1 records](#legacy-v1-records)),
so piles written before V3 read byte-identical with no migration step. New
writes are always V3. The skew direction to watch is the other one: **binaries
from before V3 treat V3 records as unknown and fail loud with
`ReadError::CorruptPile`** — they do not truncate anything. When an old binary
reports corruption on a pile a newer binary wrote, the fix is to upgrade the
binary, never to "repair" the pile.

## Design Rationale

This format emphasizes **simplicity** over sophisticated on-disk structures.
Appending new records rather than rewriting existing data keeps corruption
windows small and avoids complicated page management. Storing everything in a
single file makes a pile easy to back up, replicate over simple transports, or
merge by concatenation, while still allowing it to be memory mapped for fast
reads. Internally the pile tracks an `applied_length` watermark; offsets below
this boundary are known-good and only the tail beyond it is rescanned when
refreshing state.

## Operational workflow

1. **Open the file.** `Pile::open` builds the struct around a `File` handle
   and `memmap2` mapping. It does not read any records yet (and it does not
   create missing files — create the file explicitly for a fresh pile).
2. **Load and validate.** `refresh` acquires a shared lock, walks bytes beyond
   `applied_length`, and rebuilds the blob/pin indices in memory. It **fails
   loud** on a corrupt or torn tail (`ReadError::CorruptPile { valid_length }`)
   and never mutates the file. Callers rarely need to invoke it directly:
   `reader`, `pins`, `head`, and `update` call `refresh` internally before they
   inspect or apply records, so external writers are visible without a
   standalone scan.
3. **Repair only when asked to.** `restore` is the explicit, opt-in repair
   path: it re-runs validation under an exclusive lock and truncates the file
   back to the last valid record, discarding a torn tail left by a crash. It
   is deliberately **not** part of the normal open sequence — implicit repair
   under version skew is a silent data-loss hazard (an old binary would "eat"
   every newer-format record past the first one it misreads as corruption).
   The `trible pile restore <path>` command wraps it for operators.
4. **Append new records.** `put` (through the `BlobStorePut` trait) and pin
   update helpers extend the file via a single `write_vectored` call. Each
   append immediately feeds the bytes back through the record scanner so
   in-memory indices stay synchronised without waiting for a manual `refresh`.
   Records larger than ~1&nbsp;GiB can't be appended in a single atomic
   `writev` because kernel `write_vectored` calls cap at `INT_MAX` bytes on
   macOS and `MAX_RW_COUNT` (~2&nbsp;GiB) on Linux. In that case `put` takes
   an exclusive file lock and issues plain `write_all` calls — still
   append-only, still repairable by an explicit `restore` if a crash leaves a
   partial tail, but serialised against other writers for the duration of the
   append.
5. **Read through a snapshot.** `reader` clones the memory map and PATCH
   indices into a `PileReader`, yielding iterators and metadata lookups that
   can execute without further locking.

This lifecycle keeps pile usage predictable: open → operate (operations
refresh as they run) → hand out read-only readers. If a process wants to scan
for new appends between operations (for example, a background monitor that is
not issuing `reader` or pin calls), it can explicitly call `refresh` to pick up
external writers without blocking them for long. If corruption is ever
reported, surface it to the operator; truncating is a decision, not a default.

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

Every record begins with a 16&nbsp;byte magic marker that identifies its kind.
The sections below illustrate the layout of each type.

## Usage

A pile typically lives as a `.pile` file on disk. Repositories open it through
`Pile::open` and load it with `refresh` (directly or via the first operation
that refreshes internally). Multiple threads may share the same handle thanks
to internal synchronisation, making a pile a convenient durable store for
local development. Blob appends use a single `O_APPEND` write. Each handle
remembers the last offset it processed and, after appending, scans any gap left
by concurrent writes before advancing this `applied_length`. Writers may race
and duplicate blobs, but content addressing keeps the data consistent. Each
handle tracks hashes of pending appends separately so repeated writes are
deduplicated until a `refresh`. Pin updates only record the referenced hash and
do not verify that the corresponding blob exists in the pile, so a pile may act
as a head-only store when blob data resides elsewhere.

```rust,ignore
use std::error::Error;
use std::path::PathBuf;

use anybytes::Bytes;
use triblespace::prelude::*;
use triblespace::core::repo::pile::ReadError;
use triblespace::core::repo::BlobStoreMeta;

fn add_blob(bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from("data.pile");
    let mut pile = Pile::open(&path)?;
    // Load and validate the existing records. This FAILS LOUD on a corrupt
    // or torn tail and never mutates the file. Repair is a separate,
    // explicit decision (`Pile::restore` / `trible pile restore`), typically
    // made by an operator after checking that the binary isn't simply older
    // than the pile's records.
    match pile.refresh() {
        Ok(()) => {}
        Err(err @ ReadError::CorruptPile { .. }) => return Err(err.into()),
        Err(other) => return Err(other.into()),
    }

    // Insert a blob and obtain a handle pointing at the on-disk bytes.
    let handle = pile.put(Bytes::from_source(bytes.to_vec()))?;

    // Readers operate on a snapshot cloned from the pile's mmap.
    let reader = pile.reader()?;
    if let Some(meta) = reader.metadata(handle)? {
        println!("stored {} bytes at {}", meta.length, meta.timestamp);
    }
    drop(reader);
    pile.close()?;
    Ok(())
}
```

This pattern illustrates the typical flow: open, load with `refresh`, rely on
the built-in refreshes performed by `reader` and pin helpers, mutate via
`put`, then hand the `PileReader` snapshot to read-only consumers. Updating
pin heads requires a brief critical section—`flush → refresh → lock →
refresh → append → unlock`—so a caller observes a consistent head even when
multiple processes contend for the same file descriptor. `refresh` acquires a
shared lock so it cannot race with an explicit `restore`, which takes an
exclusive lock before truncating a corrupted tail.

Filesystems lacking atomic `write`/`vwrite` appends—such as some network or
FUSE-based implementations—cannot safely host multiple writers for records
below the `~1&nbsp;GiB` atomic-write threshold and are not supported in that
mode. (Records above the threshold use the exclusive-lock fallback and don't
rely on filesystem atomicity.) Using an atomicity-lacking filesystem for
small records risks pile corruption.

## Blob Records

```text
            ┌────16 byte───┐┌8 byte┐┌8 byte┐┌────────────32 byte───────────┐┌───192 byte───┐
          ┌ ┌──────────────┐┌──────┐┌──────┐┌──────────────────────────────┐┌──────────────┐
 header   │ │ blob marker  ││ time ││length││             hash             ││  reserved 0s │
 (256 B)  └ └──────────────┘└──────┘└──────┘└──────────────────────────────┘└──────────────┘
            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
 payload    │        bytes, post-padded so the record is a 256-byte multiple             │
            └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```

Each blob record carries:

- **Magic marker** – identifies the record kind.
- **Timestamp** – milliseconds since the Unix epoch when the append occurred.
- **Payload length** – the unpadded byte length of the blob.
- **Hash** – the digest produced by the pile's hash protocol (BLAKE3 by
  default) and used as the blob handle.
- **Reserved** – zeroed padding to the fixed 256-byte header length; not part
  of the content hash.

The payload follows at `record_start + 256` and is post-padded to the next
256-byte boundary. The [Pile Blob Metadata](./pile-blob-metadata.md) chapter
explains how to query these fields through the `PileReader` API.

## Pin Records (branch head / tombstone)

```text
            ┌────16 byte───┐┌────16 byte───┐┌────────────32 byte───────────┐┌───192 byte───┐
          ┌ ┌──────────────┐┌──────────────┐┌──────────────────────────────┐┌──────────────┐
 head     │ │ branch marker││   branch id  ││             hash             ││  reserved 0s │
 (256 B)  └ └──────────────┘└──────────────┘└──────────────────────────────┘└──────────────┘

            ┌────16 byte───┐┌────16 byte───┐┌──────────────224 byte────────────────────────┐
          ┌ ┌──────────────┐┌──────────────┐┌──────────────────────────────────────────────┐
 tombstone│ │ tomb marker  ││   branch id  ││                 reserved 0s                  │
 (256 B)  └ └──────────────┘└──────────────┘└──────────────────────────────────────────────┘
```

Pin-head records map a pin (branch) identifier to the hash of a blob; a
tombstone retracts the mapping. Appends are intentionally lightweight: the
pile does not check whether the referenced blob exists locally, allowing
deployments that store heads on disk while serving blob contents from a remote
store.

## Weak-Pin Records (want / retention markers)

```text
            ┌────16 byte───┐┌────────────32 byte───────────┐┌────────────208 byte──────────┐
          ┌ ┌──────────────┐┌──────────────────────────────┐┌──────────────────────────────┐
 weak-pin │ │  pin marker  ││         blob handle          ││          reserved 0s         │
 (256 B)  └ └──────────────┘└──────────────────────────────┘└──────────────────────────────┘
```

A weak-pin marker (and its weak-unpin counterpart, same layout with a
different marker) is keyed by **blob handle** — per-blob and anonymous, no pin
id. Together with the pin records they make retention one strength axis,
resolved last-writer-wins by log position:
`pin ⊐ weak-pin ⊐ weak-unpin ⊐ unpin` (the pin-head record *is* `pin`, the
tombstone *is* `unpin`). A weak pin is simultaneously the demand-born
want-signal ("I want this blob; fetch it if absent"), the cache-retention
marker for a fetched blob, and the eviction target under pressure. Because the
markers are durable records, reopening a pile reloads the weak set.

## Legacy V1 records

Piles written before V3 contain 64-byte-aligned records: a 64-byte blob header
(marker, timestamp, length, hash) followed by a payload padded to a 64-byte
boundary, and 64-byte branch / tombstone records. The reader recognises the V1
markers and reads these records byte-identical; they are never rewritten. V1
had no weak-pin records.

## Recovery

`refresh` scans an existing file to ensure every header uses a known marker
and that the whole record fits. It does not verify any hashes. If a truncated
or unknown block is found the function reports the number of bytes that were
valid so far using `ReadError::CorruptPile` — and leaves the file untouched.

If the file shrinks between scans into data that has already been applied, the
process aborts immediately. Previously returned `Bytes` handles would dangle
and continuing could cause undefined behavior, so truncation into validated
data is treated as unrecoverable.

`refresh` holds a shared file lock while scanning. This prevents a concurrent
`restore` call from truncating the file out from under the reader.

The `restore` helper is the explicit repair path: it re-runs the same
validation under an exclusive lock and truncates the file to the valid length
if corruption is encountered, discarding incomplete data left by an
interrupted write. Run it deliberately (e.g. via `trible pile restore <path>`)
— never as a routine part of opening — and only once you know the "corruption"
isn't just an older binary meeting newer record kinds. Hash verification
happens lazily only when individual blobs are loaded so that opening a large
pile remains fast.

For more details on interacting with a pile see the [`Pile` struct
documentation](https://docs.rs/triblespace/latest/triblespace/repo/pile/struct.Pile.html).

[1]: https://db.cs.cmu.edu/mmap-cidr2022/ "The Case Against Memory-Mapped I/O"
