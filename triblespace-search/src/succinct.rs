//! Jerky-backed succinct building blocks for the index blobs.
//!
//! **Current status:** scaffolding. This module proves the
//! `anybytes::ByteArea` → `jerky::Serializable` round-trip works
//! in this crate before we cut the full succinct `BM25Index`
//! blob over to it. Each component here is a self-contained
//! zerocopy-loadable view; later iterations compose them into
//! `SuccinctBM25Index::try_from_blob` alongside a proper
//! `try_from_blob` of the naive format.
//!
//! The module is gated behind the `succinct` feature so the
//! naive path stays compilable without `jerky`.
//!
//! # Why this exists
//!
//! The naive `BM25Index` stores `doc_lens` as `Vec<u32>` — four
//! bytes per doc regardless of how short the doc actually is. At
//! 100k docs that's 0.4 MiB, small — but the pattern generalizes.
//! Once `doc_lens` loads zero-copy through jerky's
//! `CompactVector`, the same recipe swaps in for the big levers:
//! postings (144 MiB at 100k docs), term table, HNSW neighbour
//! arrays. Get the mechanics right here with a tiny surface
//! area, then expand.
//!
//! ```
//! use triblespace_search::succinct::SuccinctDocLens;
//!
//! let lens = vec![3u32, 7, 1, 15, 2];
//! let (bytes, meta) = SuccinctDocLens::build(&lens).expect("build");
//! let view = SuccinctDocLens::from_bytes(meta, bytes).expect("load");
//!
//! assert_eq!(view.len(), 5);
//! assert_eq!(view.get(0), Some(3));
//! assert_eq!(view.get(3), Some(15));
//! assert_eq!(view.to_vec(), lens);
//! ```

use anybytes::{ByteArea, Bytes};
use jerky::int_vectors::compact_vector::CompactVectorMeta;
use jerky::int_vectors::{CompactVector, CompactVectorBuilder};
use jerky::serialization::Serializable;
use triblespace_core::blob::schemas::succinctarchive::{
    CompressedUniverse, CompressedUniverseMeta, Universe,
};
use triblespace_core::blob::{Blob, BlobSchema, ToBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::id_hex;
use triblespace_core::metadata::{ConstDescribe, ConstId};
use triblespace_core::query::Variable;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::{RawValue, Value, ValueSchema};

use crate::schemas::{EmbHandle, Embedding};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use std::collections::HashMap;
use crate::hnsw::HNSWIndex;

/// Byte-layout mirror of [`CompactVectorMeta`] that's safe to
/// serialize through our own blob format.
///
/// jerky's `CompactVectorMeta` is `FromBytes + KnownLayout +
/// Immutable` but not `IntoBytes` (it has no way to opt in from
/// downstream), so we can't `as_bytes` it directly. This mirror
/// owns the derives we need on our side, and the From/To impls
/// document + check the layout equivalence (both are `repr(C)`
/// with four `u64`-sized fields on 64-bit platforms).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct CompactVectorMetaOnDisk {
    len: u64,
    width: u64,
    handle_offset: u64,
    handle_len: u64,
}

const _: () = assert!(
    std::mem::size_of::<CompactVectorMetaOnDisk>() == 32,
    "CompactVectorMetaOnDisk must be 32 bytes",
);
const _: () = assert!(
    std::mem::size_of::<CompactVectorMeta>() == 32,
    "CompactVectorMeta must be 32 bytes (assumes 64-bit target)",
);

impl From<CompactVectorMeta> for CompactVectorMetaOnDisk {
    fn from(m: CompactVectorMeta) -> Self {
        Self {
            len: m.len as u64,
            width: m.width as u64,
            handle_offset: m.handle.offset as u64,
            handle_len: m.handle.len as u64,
        }
    }
}

/// Byte-layout mirror of triblespace's [`CompressedUniverseMeta`].
///
/// Same upstream-frozen-derives issue as [`CompactVectorMetaOnDisk`]:
/// the upstream meta derives `FromBytes` but not `IntoBytes`, so
/// we can't `as_bytes` it for our own header. Mirror the layout
/// on our side, static-assert the size, and convert via zerocopy.
///
/// Layout (all `#[repr(C)]` on 64-bit targets):
/// ```text
/// CompressedUniverseMeta:
///   fragments: SectionHandle<[u8; 4]>  (offset: usize, len: usize, phantom)
///   data:      DacsByteMeta
///     num_levels: usize
///     levels:     SectionHandle<LevelMeta>  (offset, len, phantom)
///
/// Total: 5 × 8 = 40 bytes.
/// ```
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct CompressedUniverseMetaOnDisk {
    fragments_offset: u64,
    fragments_len: u64,
    dacs_num_levels: u64,
    dacs_levels_offset: u64,
    dacs_levels_len: u64,
}

const _: () = assert!(
    std::mem::size_of::<CompressedUniverseMetaOnDisk>() == 40,
    "CompressedUniverseMetaOnDisk must be 40 bytes",
);
const _: () = assert!(
    std::mem::size_of::<CompressedUniverseMeta>() == 40,
    "CompressedUniverseMeta must be 40 bytes (assumes 64-bit target)",
);

impl CompressedUniverseMetaOnDisk {
    fn to_jerky(self) -> CompressedUniverseMeta {
        // Both structs are #[repr(C)] and size_of == 40 with
        // matching field order on 64-bit. read_from_bytes
        // validates.
        CompressedUniverseMeta::read_from_bytes(self.as_bytes())
            .expect("CompressedUniverseMeta has same 40-byte repr(C) layout on 64-bit")
    }
}

impl From<CompressedUniverseMeta> for CompressedUniverseMetaOnDisk {
    fn from(m: CompressedUniverseMeta) -> Self {
        // Upstream only derives FromBytes. Safety: both types
        // are `#[repr(C)]`, same size (static-asserted), and
        // every field is plain data.
        let mut out = Self {
            fragments_offset: 0,
            fragments_len: 0,
            dacs_num_levels: 0,
            dacs_levels_offset: 0,
            dacs_levels_len: 0,
        };
        let src_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                (&m as *const CompressedUniverseMeta) as *const u8,
                std::mem::size_of::<CompressedUniverseMeta>(),
            )
        };
        out.as_mut_bytes().copy_from_slice(src_bytes);
        out
    }
}

impl CompactVectorMetaOnDisk {
    /// Convert back to jerky's meta. Because `SectionHandle` has
    /// a private `_type: PhantomData<T>` we can't construct one
    /// with a struct literal — but its layout is identical to
    /// `(usize, usize)` and it's `#[repr(C)]` with a known layout,
    /// so we transmute through a matching byte layout.
    fn to_jerky(self) -> CompactVectorMeta {
        // Build a 32-byte scratch in the on-disk layout, then
        // `read_from_bytes` it into `CompactVectorMeta` — sound
        // because both are `repr(C)`, both are 32 bytes, and
        // both share the same field order on 64-bit targets.
        let disk_bytes = self.as_bytes();
        CompactVectorMeta::read_from_bytes(disk_bytes)
            .expect("CompactVectorMeta has same 32-byte repr(C) layout on 64-bit")
    }
}

/// Errors produced by the succinct building blocks.
#[derive(Debug)]
pub enum SuccinctDocLensError {
    /// Failure propagating out of `anybytes::ByteArea`.
    Bytes(std::io::Error),
    /// Failure propagating out of `jerky` (build or view).
    Jerky(jerky::error::Error),
    /// Declared row count does not match the byte length.
    SizeMismatch {
        /// Total bytes available.
        bytes: usize,
        /// Declared rows × row width.
        expected: usize,
    },
}

impl std::fmt::Display for SuccinctDocLensError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes(e) => write!(f, "succinct: bytes error: {e}"),
            Self::Jerky(e) => write!(f, "succinct: jerky error: {e}"),
            Self::SizeMismatch { bytes, expected } => write!(
                f,
                "succinct: size mismatch: have {bytes} bytes, declared needs {expected}"
            ),
        }
    }
}

impl std::error::Error for SuccinctDocLensError {}

impl From<std::io::Error> for SuccinctDocLensError {
    fn from(e: std::io::Error) -> Self {
        Self::Bytes(e)
    }
}

impl From<jerky::error::Error> for SuccinctDocLensError {
    fn from(e: jerky::error::Error) -> Self {
        Self::Jerky(e)
    }
}

/// A zero-copy view over per-document length counts, bit-packed
/// via [`jerky::int_vectors::CompactVector`].
///
/// The bit-width is chosen at build time as `ceil(log2(max+1))`,
/// so short-doc corpora (common for wiki fragments) pay a fraction
/// of the `u32` cost.
#[derive(Debug)]
pub struct SuccinctDocLens {
    inner: CompactVector,
}

impl SuccinctDocLens {
    /// Build a succinct doc-lens table. Returns the frozen
    /// [`Bytes`] region and [`CompactVectorMeta`] needed to
    /// reconstruct a view with [`Self::from_bytes`].
    pub fn build(lens: &[u32]) -> Result<(Bytes, CompactVectorMeta), SuccinctDocLensError> {
        let width = required_width(lens);
        let mut area = ByteArea::new()?;
        let mut sections = area.sections();
        let mut builder = CompactVectorBuilder::with_capacity(lens.len(), width, &mut sections)?;
        builder.set_ints(0..lens.len(), lens.iter().map(|&n| n as usize))?;
        let cv = builder.freeze();
        let meta = cv.metadata();
        let bytes = area.freeze()?;
        Ok((bytes, meta))
    }

    /// Reconstruct a view from the frozen bytes + metadata.
    pub fn from_bytes(meta: CompactVectorMeta, bytes: Bytes) -> Result<Self, SuccinctDocLensError> {
        let inner = CompactVector::from_bytes(meta, bytes)?;
        Ok(Self { inner })
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if there are no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    /// Document length at position `i`, or `None` if out of range.
    pub fn get(&self, i: usize) -> Option<u32> {
        // get_int returns usize; doc_lens fit in u32 by construction.
        self.inner.get_int(i).map(|n| n as u32)
    }

    /// Collect into a `Vec<u32>` for ergonomic inspection.
    pub fn to_vec(&self) -> Vec<u32> {
        self.inner.to_vec().into_iter().map(|n| n as u32).collect()
    }

    /// Bit width per entry (log₂ of max value + 1).
    pub fn width(&self) -> usize {
        self.inner.metadata().width
    }
}

/// Number of bits needed to represent every entry in `lens`.
/// Always at least 1 so `CompactVectorBuilder::with_capacity`
/// accepts it (width ∈ `1..=64`).
fn required_width(lens: &[u32]) -> usize {
    let max = lens.iter().copied().max().unwrap_or(0);
    match max {
        0 => 1,
        _ => 32 - max.leading_zeros() as usize,
    }
}

/// Fixed-row-size table of bytes, zero-copy loaded from a
/// contiguous [`Bytes`] region.
///
/// Parameterized on the row width `N`. Used for BM25's doc-id
/// table (`N = 16`) and term table (`N = 32`). No jerky
/// compression — each row is already fixed-size and can be
/// sliced out of the raw bytes with a pointer, which is what
/// makes this "succinct" at all: the caller's `Bytes` handle is
/// the ground truth and no data is duplicated into structs.
///
/// The caller is responsible for sorting the table at build time
/// if they want binary-search lookups (see [`Self::binary_search`]).
///
/// # Build / view
///
/// ```
/// use triblespace_search::succinct::FixedBytesTable;
///
/// let rows = vec![[1u8; 16], [3u8; 16], [7u8; 16]];
/// let bytes = FixedBytesTable::<16>::build(&rows);
/// let view = FixedBytesTable::<16>::from_bytes(bytes, rows.len()).unwrap();
/// assert_eq!(view.binary_search(&[3u8; 16]), Ok(1));
/// assert_eq!(view.binary_search(&[5u8; 16]), Err(2));
/// ```
#[derive(Debug)]
pub struct FixedBytesTable<const N: usize> {
    bytes: Bytes,
    len: usize,
}

impl<const N: usize> FixedBytesTable<N> {
    /// Serialize `rows` into a flat `Bytes` buffer. The companion
    /// [`Self::from_bytes`] reconstructs a view; callers persist
    /// `rows.len()` out-of-band (e.g., in a BlobHeader).
    pub fn build(rows: &[[u8; N]]) -> Bytes {
        let mut buf = Vec::with_capacity(rows.len() * N);
        for row in rows {
            buf.extend_from_slice(row);
        }
        Bytes::from_source(buf)
    }

    /// View `bytes` as `len` rows of `N` bytes each.
    pub fn from_bytes(bytes: Bytes, len: usize) -> Result<Self, SuccinctDocLensError> {
        let expected = len
            .checked_mul(N)
            .ok_or(SuccinctDocLensError::SizeMismatch {
                bytes: bytes.len(),
                expected: usize::MAX,
            })?;
        if bytes.len() < expected {
            return Err(SuccinctDocLensError::SizeMismatch {
                bytes: bytes.len(),
                expected,
            });
        }
        Ok(Self { bytes, len })
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// `true` if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Row at position `i`, or `None` if out of range. Zero-copy:
    /// returns a reference into the backing `Bytes`.
    pub fn get(&self, i: usize) -> Option<&[u8; N]> {
        if i >= self.len {
            return None;
        }
        let start = i * N;
        let end = start + N;
        // A byte array has trivial alignment, so the slice
        // directly coerces to `&[u8; N]`.
        self.bytes[start..end].try_into().ok()
    }

    /// Binary-search for `needle`. Returns `Ok(index)` if found,
    /// `Err(insertion_point)` otherwise. Requires the table to be
    /// sorted at build time.
    pub fn binary_search(&self, needle: &[u8; N]) -> std::result::Result<usize, usize> {
        let mut lo = 0;
        let mut hi = self.len;
        while lo < hi {
            let mid = (lo + hi) / 2;
            // `.expect` is safe: mid < hi ≤ self.len, and get()
            // validates against that.
            let row = self.get(mid).expect("binary search in-bounds");
            match row.as_slice().cmp(needle.as_slice()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Collect into a `Vec<[u8; N]>` for tests / inspection.
    pub fn to_vec(&self) -> Vec<[u8; N]> {
        (0..self.len).map(|i| *self.get(i).unwrap()).collect()
    }
}

/// Per-term posting lists backed by jerky primitives.
///
/// Three [`CompactVector`]s sharing one [`anybytes::ByteArea`]:
/// - `doc_idx`: per-posting document index, width
///   `ceil(log2(n_docs + 1))`.
/// - `offsets`: per-term cumulative offsets into `doc_idx`, width
///   `ceil(log2(total + 1))`.
/// - `scores`: u16-quantized score bucket per posting, width 16.
///
/// Quantization: each original f32 score is mapped to a u16
/// bucket via `q = round(s / max_score * 65535)`, and dequantized
/// back to f32 via `q * max_score / 65535`. The `max_score`
/// scalar is stored in [`SuccinctPostingsMeta`]. The
/// half-bucket error floor is `max_score / 2 * 65535`, and the
/// `scale_for_equality = max_score / 65534` value is what
/// callers should use as an equality tolerance against a
/// bound score variable.
///
/// [`build`] returns the combined `Bytes` plus metadata. The
/// caller decides where it lands in the final blob.
///
/// [`build`]: Self::build
#[derive(Debug)]
pub struct SuccinctPostings {
    doc_idx: CompactVector,
    offsets: CompactVector,
    scores: CompactVector,
    max_score: f32,
    n_terms: usize,
}

/// Serialized layout metadata for [`SuccinctPostings`].
#[derive(Debug, Clone, Copy)]
pub struct SuccinctPostingsMeta {
    /// Meta for the `doc_idx` CompactVector.
    pub doc_idx: CompactVectorMeta,
    /// Meta for the `offsets` CompactVector.
    pub offsets: CompactVectorMeta,
    /// Meta for the `scores` CompactVector (u16-quantized).
    pub scores: CompactVectorMeta,
    /// Quantization scale: the largest original score in the
    /// corpus. Zero when no postings or all zeros.
    pub max_score: f32,
    /// Number of terms (== `offsets.len() - 1`).
    pub n_terms: u64,
}

/// u16 quantization width.
const SCORE_WIDTH: usize = 16;
/// Highest u16 value.
const SCORE_MAX_Q: u32 = u16::MAX as u32;

/// Quantize a single f32 score to its u16 bucket given the
/// corpus `max_score`. When `max_score == 0` (empty corpus, all
/// scores zero) the bucket is always zero.
fn quantize_score(s: f32, max_score: f32) -> u16 {
    if max_score <= 0.0 {
        return 0;
    }
    // Clamp in case of tiny numerical drift above max_score.
    let ratio = (s / max_score).clamp(0.0, 1.0);
    (ratio * SCORE_MAX_Q as f32).round() as u16
}

/// Dequantize a u16 bucket back to an approximate f32 score.
fn dequantize_score(q: u16, max_score: f32) -> f32 {
    if max_score <= 0.0 {
        return 0.0;
    }
    (q as f32 / SCORE_MAX_Q as f32) * max_score
}

impl SuccinctPostings {
    /// Serialize `lists[t]` as the posting list for term index
    /// `t`. Returns `(bytes, meta)`. `n_docs` is the total
    /// document count (sets the bit width for `doc_idx`).
    pub fn build(
        lists: &[Vec<(u32, f32)>],
        n_docs: u32,
    ) -> Result<(Bytes, SuccinctPostingsMeta), SuccinctDocLensError> {
        Self::build_with(n_docs, lists.len(), |t, buf| {
            buf.extend_from_slice(&lists[t]);
        })
    }

    /// Streaming build — caller materializes one term's posting
    /// list at a time into a reused buffer instead of allocating
    /// `Vec<Vec<(u32, f32)>>` upfront. The closure is invoked
    /// twice per term: once during the size + max-score pass, once
    /// during the byte-write pass. It must be deterministic across
    /// invocations (the bytes written in the second pass must
    /// match the sizes / scores observed in the first) and clear
    /// or overwrite `buf` itself if reuse semantics matter.
    /// `materialize_term` receives `buf` empty on each call.
    ///
    /// Memory profile: peak temporary vec holds one term's
    /// postings instead of every term's at once. At 100 k docs
    /// with Heaps-law vocabulary that's ~400 KB instead of
    /// ~144 MB.
    pub fn build_with<F>(
        n_docs: u32,
        n_terms: usize,
        mut materialize_term: F,
    ) -> Result<(Bytes, SuccinctPostingsMeta), SuccinctDocLensError>
    where
        F: FnMut(usize, &mut Vec<(u32, f32)>),
    {
        // ── Pass 1: total posting count + max_score. ─────────────
        let mut buf: Vec<(u32, f32)> = Vec::new();
        let mut total: usize = 0;
        let mut max_score = 0.0f32;
        for t in 0..n_terms {
            buf.clear();
            materialize_term(t, &mut buf);
            total += buf.len();
            for &(_, s) in &buf {
                if s > max_score {
                    max_score = s;
                }
            }
        }

        let doc_idx_width = width_for(n_docs as usize + 1);
        let offsets_width = width_for(total + 1);

        let mut area = ByteArea::new()?;
        let mut sections = area.sections();

        let mut doc_idx_b =
            CompactVectorBuilder::with_capacity(total, doc_idx_width, &mut sections)?;
        let mut offsets_b =
            CompactVectorBuilder::with_capacity(n_terms + 1, offsets_width, &mut sections)?;
        let mut scores_b = CompactVectorBuilder::with_capacity(total, SCORE_WIDTH, &mut sections)?;
        offsets_b.set_int(0, 0)?;

        // ── Pass 2: write all three CompactVectors. ──────────────
        let mut pos = 0usize;
        for t in 0..n_terms {
            buf.clear();
            materialize_term(t, &mut buf);
            for &(idx, s) in &buf {
                doc_idx_b.set_int(pos, idx as usize)?;
                scores_b.set_int(pos, quantize_score(s, max_score) as usize)?;
                pos += 1;
            }
            offsets_b.set_int(t + 1, pos)?;
        }

        let doc_idx = doc_idx_b.freeze();
        let doc_idx_meta = doc_idx.metadata();
        let offsets = offsets_b.freeze();
        let offsets_meta = offsets.metadata();
        let scores = scores_b.freeze();
        let scores_meta = scores.metadata();

        // Release the section handles so `area.freeze()` can move
        // the ByteArea; clippy flags `drop()` on non-Drop types.
        let _ = (doc_idx, offsets, scores);
        let _ = sections;
        let bytes = area.freeze()?;

        let meta = SuccinctPostingsMeta {
            doc_idx: doc_idx_meta,
            offsets: offsets_meta,
            scores: scores_meta,
            max_score,
            n_terms: n_terms as u64,
        };
        Ok((bytes, meta))
    }

    /// Reconstruct from metadata + the combined byte region.
    pub fn from_bytes(
        meta: SuccinctPostingsMeta,
        bytes: Bytes,
    ) -> Result<Self, SuccinctDocLensError> {
        let doc_idx = CompactVector::from_bytes(meta.doc_idx, bytes.clone())?;
        let offsets = CompactVector::from_bytes(meta.offsets, bytes.clone())?;
        let scores = CompactVector::from_bytes(meta.scores, bytes)?;
        Ok(Self {
            doc_idx,
            offsets,
            scores,
            max_score: meta.max_score,
            n_terms: meta.n_terms as usize,
        })
    }

    /// Number of terms.
    pub fn term_count(&self) -> usize {
        self.n_terms
    }

    /// Corpus max score — the quantization scale.
    pub fn max_score(&self) -> f32 {
        self.max_score
    }

    /// Equality tolerance callers should use when matching a
    /// bound score variable against stored values. Derived from
    /// the quantization bucket size: `max_score / 65534`.
    pub fn score_tolerance(&self) -> f32 {
        if self.max_score <= 0.0 {
            f32::EPSILON
        } else {
            self.max_score / 65534.0
        }
    }

    /// Number of postings for term `t`. `None` if out of range.
    pub fn posting_count(&self, t: usize) -> Option<usize> {
        if t >= self.n_terms {
            return None;
        }
        let start = self.offsets.get_int(t)?;
        let end = self.offsets.get_int(t + 1)?;
        Some(end - start)
    }

    /// Iterate `(doc_idx, score)` postings for term `t`. Scores
    /// are dequantized from their u16 buckets.
    pub fn postings_for(&self, t: usize) -> Option<impl Iterator<Item = (u32, f32)> + '_> {
        if t >= self.n_terms {
            return None;
        }
        let start = self.offsets.get_int(t)?;
        let end = self.offsets.get_int(t + 1)?;
        let max = self.max_score;
        Some((start..end).map(move |i| {
            let idx = self.doc_idx.get_int(i).unwrap() as u32;
            let q = self.scores.get_int(i).unwrap() as u16;
            (idx, dequantize_score(q, max))
        }))
    }
}

/// Minimum bit width to represent the value `n` (or 1 if 0).
fn width_for(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        (usize::BITS - (n - 1).leading_zeros()) as usize
    }
}

/// Jerky-backed HNSW layer-graph component.
///
/// Flat CSR over `(layer, node) → [neighbour_node_idx, ...]`:
/// - `neighbours`: [`CompactVector`] of neighbour node indices
///   across all (layer, node) pairs, width
///   `ceil(log2(n_nodes + 1))`.
/// - `offsets`: [`CompactVector`] with `n_layers × (n_nodes + 1)`
///   entries giving cumulative starts into `neighbours`. Width
///   `ceil(log2(total_edges + 1))`.
///
/// For a node `i` at layer `L`, its neighbour list spans
/// `[offsets[L·(n+1) + i] .. offsets[L·(n+1) + i + 1])` in
/// `neighbours`. Nodes that weren't promoted to layer `L` have an
/// empty slice there — safe to walk, never traversed by a correct
/// search.
///
/// This is the building block the eventual
/// `SuccinctHNSWIndex::try_from_blob` will consume per the RING
/// plan in `docs/DESIGN.md` (no labels, so one wavelet matrix
/// per layer would be even more compact, but CSR keeps the
/// first-cut surface small and debuggable).
#[derive(Debug)]
pub struct SuccinctGraph {
    neighbours: CompactVector,
    offsets: CompactVector,
    n_nodes: usize,
    n_layers: usize,
}

/// Serialized layout metadata for [`SuccinctGraph`].
#[derive(Debug, Clone, Copy)]
pub struct SuccinctGraphMeta {
    /// Meta for `neighbours`.
    pub neighbours: CompactVectorMeta,
    /// Meta for `offsets`.
    pub offsets: CompactVectorMeta,
    /// Number of nodes in the graph.
    pub n_nodes: u64,
    /// Number of layers (0..n_layers; layer 0 is the full graph).
    pub n_layers: u64,
}

impl SuccinctGraph {
    /// Serialize the layer-major neighbour lists.
    /// `layer_graph[L][i]` = neighbours of node `i` on layer `L`.
    /// Every node must have an entry at every layer (possibly
    /// empty) so offsets stay aligned.
    pub fn build(
        layer_graph: &[Vec<Vec<u32>>],
        n_nodes: usize,
    ) -> Result<(Bytes, SuccinctGraphMeta), SuccinctDocLensError> {
        let n_layers = layer_graph.len();
        // Sanity: every layer must have `n_nodes` entries.
        for layer in layer_graph {
            if layer.len() != n_nodes {
                return Err(SuccinctDocLensError::SizeMismatch {
                    bytes: layer.len(),
                    expected: n_nodes,
                });
            }
            // Out-of-range neighbour index → refuse to build.
            for list in layer {
                for &n in list {
                    if (n as usize) >= n_nodes {
                        return Err(SuccinctDocLensError::SizeMismatch {
                            bytes: n as usize,
                            expected: n_nodes,
                        });
                    }
                }
            }
        }
        let total_edges: usize = layer_graph
            .iter()
            .flat_map(|layer| layer.iter().map(|l| l.len()))
            .sum();
        let neighbours_width = width_for(n_nodes + 1);
        let offsets_width = width_for(total_edges + 1);
        let offsets_len = n_layers * (n_nodes + 1);

        let mut area = ByteArea::new()?;
        let mut sections = area.sections();

        let mut neighbours_b =
            CompactVectorBuilder::with_capacity(total_edges, neighbours_width, &mut sections)?;
        let mut pos = 0usize;
        for layer in layer_graph {
            for list in layer {
                for &n in list {
                    neighbours_b.set_int(pos, n as usize)?;
                    pos += 1;
                }
            }
        }
        let neighbours = neighbours_b.freeze();
        let neighbours_meta = neighbours.metadata();

        let mut offsets_b =
            CompactVectorBuilder::with_capacity(offsets_len, offsets_width, &mut sections)?;
        let mut cum = 0usize;
        let mut slot = 0usize;
        for layer in layer_graph {
            offsets_b.set_int(slot, cum)?;
            slot += 1;
            for list in layer {
                cum += list.len();
                offsets_b.set_int(slot, cum)?;
                slot += 1;
            }
        }
        // Fill any trailing slots (if n_layers == 0, offsets_len
        // is 0 and the loop was a no-op).
        while slot < offsets_len {
            offsets_b.set_int(slot, cum)?;
            slot += 1;
        }
        let offsets = offsets_b.freeze();
        let offsets_meta = offsets.metadata();

        let _ = (neighbours, offsets);
        let _ = sections;
        let bytes = area.freeze()?;

        let meta = SuccinctGraphMeta {
            neighbours: neighbours_meta,
            offsets: offsets_meta,
            n_nodes: n_nodes as u64,
            n_layers: n_layers as u64,
        };
        Ok((bytes, meta))
    }

    /// Reconstruct from bytes + metadata.
    pub fn from_bytes(meta: SuccinctGraphMeta, bytes: Bytes) -> Result<Self, SuccinctDocLensError> {
        let neighbours = CompactVector::from_bytes(meta.neighbours, bytes.clone())?;
        let offsets = CompactVector::from_bytes(meta.offsets, bytes)?;
        Ok(Self {
            neighbours,
            offsets,
            n_nodes: meta.n_nodes as usize,
            n_layers: meta.n_layers as usize,
        })
    }

    /// Number of nodes.
    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }
    /// Number of layers.
    pub fn n_layers(&self) -> usize {
        self.n_layers
    }

    /// Iterate neighbours of `node` on `layer`. Empty iterator if
    /// either index is out of range (matching the naive index's
    /// "no list at layer > node.level" semantics).
    pub fn neighbours(&self, node: usize, layer: usize) -> impl Iterator<Item = u32> + '_ {
        let (start, end) = if node >= self.n_nodes || layer >= self.n_layers {
            (0usize, 0usize)
        } else {
            let slot = layer * (self.n_nodes + 1) + node;
            let start = self.offsets.get_int(slot).unwrap_or(0);
            let end = self.offsets.get_int(slot + 1).unwrap_or(start);
            (start, end)
        };
        (start..end).map(move |i| self.neighbours.get_int(i).unwrap() as u32)
    }
}

/// Zero-copy, jerky-backed HNSW index.
///
/// Same query surface as [`HNSWIndex`] (Malkov-Yashunin greedy
/// descent + ef-search, threshold-gated via
/// `candidates_above(handle, floor)`), but the graph lives in a
/// [`SuccinctGraph`] (bit-packed CSR over (layer, node) →
/// neighbours) and nodes are `Value<Handle<Blake3, Embedding>>`
/// in a [`FixedBytesTable<32>`]. Embeddings live in the pile's
/// blob store, content-addressed — queries resolve handles
/// through the attached reader at walk time.
///
/// Built via [`Self::from_naive`]; a direct builder skipping the
/// naive intermediate is a later optimization.
///
/// # Example
///
/// ```
/// use triblespace_core::blob::MemoryBlobStore;
/// use triblespace_core::repo::BlobStore;
/// use triblespace_core::value::schemas::hash::Blake3;
/// use triblespace_search::hnsw::HNSWBuilder;
/// use triblespace_search::schemas::put_embedding;
/// use triblespace_search::succinct::SuccinctHNSWIndex;
///
/// let mut store = MemoryBlobStore::<Blake3>::new();
/// let mut b = HNSWBuilder::new(4).with_seed(1);
/// let mut handles = Vec::new();
/// for v in [
///     vec![1.0f32, 0.0, 0.0, 0.0],
///     vec![0.0, 1.0, 0.0, 0.0],
///     vec![0.9, 0.1, 0.0, 0.0],
/// ] {
///     let h = put_embedding::<_, Blake3>(&mut store, v.clone()).unwrap();
///     b.insert(h, v).unwrap();
///     handles.push(h);
/// }
/// let idx: SuccinctHNSWIndex = b.build();
///
/// let reader = store.reader().unwrap();
/// let hits = idx.attach(&reader).candidates_above(handles[0], 0.8).unwrap();
/// assert!(hits.contains(&handles[0]));
/// assert!(hits.contains(&handles[2]));  // 0.9*1 + 0.1*0 ≈ 0.994
/// ```
pub struct SuccinctHNSWIndex {
    dim: usize,
    m: u16,
    m0: u16,
    max_level: u8,
    entry_point: Option<u32>,
    /// Content-addressed pointer to each node's [`Embedding`]
    /// blob. The node IS the handle — no separate doc-key
    /// identity. Distance evaluations resolve handles through
    /// a caller-supplied [`BlobStoreGet`][g] at query time.
    ///
    /// [g]: triblespace_core::repo::BlobStoreGet
    handles: FixedBytesTable<32>,
    graph: SuccinctGraph,
}

impl std::fmt::Debug for SuccinctHNSWIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuccinctHNSWIndex")
            .field("n_nodes", &self.handles.len())
            .field("dim", &self.dim)
            .field("max_level", &self.max_level)
            .finish()
    }
}

impl SuccinctHNSWIndex {
    /// Re-encode a naive [`HNSWIndex`] into the succinct form.
    pub fn from_naive(idx: &HNSWIndex) -> Result<Self, SuccinctDocLensError> {
        let n = idx.doc_count();
        let dim = idx.dim();
        let max_level = idx.max_level();
        let n_layers = max_level as usize + 1;

        // handles: 32-byte content-addressed pointers to
        // Embedding blobs.
        let handle_rows: Vec<RawValue> = idx.handles().iter().map(|h| h.raw).collect();
        let handles_bytes = FixedBytesTable::<32>::build(&handle_rows);
        let handles = FixedBytesTable::<32>::from_bytes(handles_bytes, n)?;

        // Build layer-major graph: layer_graph[L][i] = neighbours.
        // Empty lists are fine for nodes not promoted to layer L —
        // the search walks through them as dead ends.
        let mut layer_graph: Vec<Vec<Vec<u32>>> = (0..n_layers)
            .map(|_| (0..n).map(|_| Vec::new()).collect())
            .collect();
        for (layer, row) in layer_graph.iter_mut().enumerate() {
            for (i, slot) in row.iter_mut().enumerate() {
                let lvl = idx.node_level(i).expect("node in range") as usize;
                if lvl >= layer {
                    *slot = idx.node_neighbours(i, layer as u8).to_vec();
                }
            }
        }
        let (graph_bytes, graph_meta) = SuccinctGraph::build(&layer_graph, n)?;
        let graph = SuccinctGraph::from_bytes(graph_meta, graph_bytes)?;

        Ok(Self {
            dim,
            m: idx.m(),
            m0: idx.m0(),
            max_level,
            entry_point: idx.entry_point(),
            handles,
            graph,
        })
    }

    /// Vector dimensionality.
    pub fn dim(&self) -> usize {
        self.dim
    }
    /// Number of nodes in the graph.
    pub fn doc_count(&self) -> usize {
        self.handles.len()
    }
    /// Max neighbours per non-zero layer.
    pub fn m(&self) -> u16 {
        self.m
    }
    /// Max neighbours on layer 0.
    pub fn m0(&self) -> u16 {
        self.m0
    }
    /// Highest layer any node was promoted to.
    pub fn max_level(&self) -> u8 {
        self.max_level
    }

    /// Attach a blob store to this index, returning a queryable
    /// view. Paired with the typical load flow:
    ///
    /// ```ignore
    /// let idx: SuccinctHNSWIndex = reader.get(handle)?;
    /// let view = idx.attach(&reader);
    /// view.candidates_above(from_handle, 0.8)?;
    /// ```
    ///
    /// The view wraps `store` in an internal
    /// [`BlobCache`][c] keyed on `Handle<Blake3, Embedding>`,
    /// so the HNSW walk's repeat visits to the same node
    /// deserialize each embedding at most once per view.
    /// `B: Clone` so the cache can own the store; typical
    /// readers are cheap-clone.
    ///
    /// [c]: triblespace_core::blob::BlobCache
    pub fn attach<'a, B>(&'a self, store: &B) -> AttachedSuccinctHNSWIndex<'a, B>
    where
        B: triblespace_core::repo::BlobStoreGet<Blake3> + Clone,
    {
        AttachedSuccinctHNSWIndex {
            index: self,
            cache: triblespace_core::blob::BlobCache::new(store.clone()),
            ef_search: 200,
        }
    }

    /// Serialize to a self-contained blob. Layout:
    ///
    /// ```text
    /// [header 152 B]
    ///   magic u32     = "SH25"
    ///   version u16   (FORMAT_VERSION)
    ///   reserved u16
    ///   dim u32                  ; embedding dimensionality
    ///   M u16, M0 u16            ; HNSW degree caps
    ///   max_level u8
    ///   reserved u8
    ///   has_entry_point u8
    ///   reserved u8
    ///   entry_point u32
    ///   n_nodes u64
    ///   n_layers u64
    ///   graph_neighbours_meta  32 B   ; CompactVectorMetaOnDisk
    ///   graph_offsets_meta     32 B   ; CompactVectorMetaOnDisk
    ///   (section_offset, section_len) × 3 = 48 B
    ///
    /// [keys    ] n_nodes × 32 B    ; FixedBytesTable<32> raw;
    ///                                each entry is the doc's
    ///                                `RawValue` identifier
    ///                                (GenId / ShortString / …).
    /// [handles ] n_nodes × 32 B    ; Value<Handle<Blake3, Embedding>>;
    ///                                content-addressed pointer
    ///                                to the doc's embedding blob
    ///                                in the pile's blob store.
    /// [graph   ] variable          ; SuccinctGraph body = two
    ///                                jerky CompactVectors in one
    ///                                ByteArea:
    ///                                  neighbours (width log₂(n+1))
    ///                                  offsets    (width log₂(E+1))
    /// ```
    ///
    /// The header carries scalar HNSW parameters, the graph's
    /// two `CompactVectorMeta` structures, and `(offset, length)`
    /// pairs for each body section.
    ///
    /// Embeddings are **not** in this blob — they live in the
    /// pile's blob store, referenced by handle. Queries resolve
    /// handles at traversal time through
    /// [`AttachedSuccinctHNSWIndex`]'s internal `BlobCache`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n_nodes = self.doc_count() as u64;
        let n_layers = self.graph.n_layers() as u64;

        // Handles section: every row is exactly 32 bytes
        // (FixedBytesTable<32>), so the section size is computable
        // up front. We stream rows from the existing view directly
        // into the output buffer at write time, dropping a triple
        // allocation (rows Vec + ByteArea + flat copy ≈ 10 MB at
        // 100 k nodes) that the previous round-trip cost.
        let handles_len = self.doc_count() as u64 * 32;

        // Rebuild the graph from the current view so we get fresh
        // bytes with offsets starting at 0 inside the region.
        let mut layer_graph: Vec<Vec<Vec<u32>>> = (0..self.graph.n_layers())
            .map(|_| {
                (0..self.graph.n_nodes())
                    .map(|_| Vec::new())
                    .collect::<Vec<Vec<u32>>>()
            })
            .collect();
        for (l, row) in layer_graph.iter_mut().enumerate() {
            for (i, slot) in row.iter_mut().enumerate() {
                *slot = self.graph.neighbours(i, l).collect();
            }
        }
        let (graph_region, graph_meta) =
            SuccinctGraph::build(&layer_graph, self.graph.n_nodes()).expect("re-serialize graph");
        let graph_neighbours_meta: CompactVectorMetaOnDisk = graph_meta.neighbours.into();
        let graph_offsets_meta: CompactVectorMetaOnDisk = graph_meta.offsets.into();

        // Section offsets inside the body (relative to end of
        // header).
        let handles_off = 0u64;
        let graph_off = handles_off + handles_len;
        let graph_len = graph_region.len() as u64;

        let body_len = graph_off + graph_len;
        let mut buf = Vec::with_capacity(SH25_HEADER_LEN + body_len as usize);

        // ── header (128 B) ────────────────────────────────────
        buf.extend_from_slice(&(self.dim as u32).to_le_bytes()); // 4
        buf.extend_from_slice(&self.m.to_le_bytes()); // 2
        buf.extend_from_slice(&self.m0.to_le_bytes()); // 2
        buf.push(self.max_level); // 1
        buf.push(0u8); // reserved, 1
        buf.push(self.entry_point.is_some() as u8); // 1
        buf.push(0u8); // reserved, 1
        let ep = self.entry_point.unwrap_or(u32::MAX);
        buf.extend_from_slice(&ep.to_le_bytes()); // 4
        buf.extend_from_slice(&n_nodes.to_le_bytes()); // 8
        buf.extend_from_slice(&n_layers.to_le_bytes()); // 8
        buf.extend_from_slice(graph_neighbours_meta.as_bytes()); // 32
        buf.extend_from_slice(graph_offsets_meta.as_bytes()); // 32
        buf.extend_from_slice(&handles_off.to_le_bytes()); // 8
        buf.extend_from_slice(&handles_len.to_le_bytes()); // 8
        buf.extend_from_slice(&graph_off.to_le_bytes()); // 8
        buf.extend_from_slice(&graph_len.to_le_bytes()); // 8
        debug_assert_eq!(buf.len(), SH25_HEADER_LEN);

        // ── body ──────────────────────────────────────────────
        // Handles: stream rows directly from the current view —
        // no Vec<RawValue> rebuild, no ByteArea round-trip.
        for i in 0..self.doc_count() {
            let row = self.handles.get(i).copied().unwrap_or([0u8; 32]);
            buf.extend_from_slice(&row);
        }
        buf.extend_from_slice(&graph_region);
        buf
    }

    /// Reload from bytes previously produced by [`Self::to_bytes`].
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, SuccinctLoadError> {
        if bytes.len() < SH25_HEADER_LEN {
            return Err(SuccinctLoadError::ShortHeader);
        }
        let dim = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let m = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let m0 = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        let max_level = bytes[8];
        let has_ep = bytes[10] != 0;
        let ep_raw = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let entry_point = if has_ep { Some(ep_raw) } else { None };
        let n_nodes = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let _n_layers = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;

        let graph_neighbours_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[32..64])
            .map_err(|_| SuccinctLoadError::BadMeta("graph.neighbours"))?
            .to_jerky();
        let graph_offsets_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[64..96])
            .map_err(|_| SuccinctLoadError::BadMeta("graph.offsets"))?
            .to_jerky();

        let read_u64 = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
        let handles_off = read_u64(96) as usize;
        let handles_len = read_u64(104) as usize;
        let graph_off = read_u64(112) as usize;
        let graph_len = read_u64(120) as usize;
        debug_assert_eq!(SH25_HEADER_LEN, 128);
        let _ = dim; // dim stays header-only; no inline vectors.

        let body_start = SH25_HEADER_LEN;
        let body = &bytes[body_start..];
        let check = |end: usize, name: &'static str| -> Result<(), SuccinctLoadError> {
            if end > body.len() {
                Err(SuccinctLoadError::TruncatedSection(name))
            } else {
                Ok(())
            }
        };
        check(handles_off + handles_len, "handles")?;
        check(graph_off + graph_len, "graph")?;

        let body_bytes = Bytes::from_source(body.to_vec());
        let handles_bytes = body_bytes.slice(handles_off..handles_off + handles_len);
        let graph_bytes = body_bytes.slice(graph_off..graph_off + graph_len);

        let handles = FixedBytesTable::<32>::from_bytes(handles_bytes, n_nodes)
            .map_err(|_| SuccinctLoadError::TruncatedSection("handles"))?;

        let graph_meta = SuccinctGraphMeta {
            neighbours: graph_neighbours_meta,
            offsets: graph_offsets_meta,
            n_nodes: n_nodes as u64,
            n_layers: _n_layers as u64,
        };
        let graph = SuccinctGraph::from_bytes(graph_meta, graph_bytes)
            .map_err(|_| SuccinctLoadError::TruncatedSection("graph"))?;

        Ok(Self {
            dim,
            m,
            m0,
            max_level,
            entry_point,
            handles,
            graph,
        })
    }

}

/// A [`SuccinctHNSWIndex`] paired with the blob store its
/// handles resolve against — produced by
/// [`SuccinctHNSWIndex::attach`]. All `similar_*` methods and
/// the query constraints live here; the bare
/// [`SuccinctHNSWIndex`] only exposes metadata and the blob
/// format.
///
/// The view owns a [`BlobCache`][c] over the provided store,
/// specialized to `(Embedding, View<[f32]>)`. HNSW walks
/// revisit neighbour nodes repeatedly — the cache collapses
/// those into a single blob-fetch + deserialize per node per
/// view.
///
/// [c]: triblespace_core::blob::BlobCache
pub struct AttachedSuccinctHNSWIndex<'a, B>
where
    B: triblespace_core::repo::BlobStoreGet<Blake3>,
{
    index: &'a SuccinctHNSWIndex,
    cache: triblespace_core::blob::BlobCache<B, Blake3, Embedding, anybytes::View<[f32]>>,
    ef_search: usize,
}

impl<'a, B> AttachedSuccinctHNSWIndex<'a, B>
where
    B: triblespace_core::repo::BlobStoreGet<Blake3>,
{
    /// Back-reference to the inner index.
    pub fn index(&self) -> &SuccinctHNSWIndex {
        self.index
    }

    /// Override the search-beam width. Larger values trade
    /// compute for recall on high-threshold queries. Default 200.
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }

    /// Build a symmetric similarity constraint over two handle
    /// variables, gated by a fixed cosine `score_floor`. See
    /// [`crate::constraint::Similar`] for semantics and the full
    /// `find!` / `pattern!` integration.
    pub fn similar(
        &self,
        a: Variable<EmbHandle>,
        b: Variable<EmbHandle>,
        score_floor: f32,
    ) -> crate::constraint::Similar<'_, Self> {
        crate::constraint::Similar::new(self, a, b, score_floor)
    }

    /// Convenience wrapper for the common "search from a known
    /// handle" case. Walks the graph once at construction from
    /// `probe`, stores the above-threshold handles, and binds
    /// `var` to them in the engine. Equivalent to
    /// `temp!((a), and!(a.is(probe), self.similar(a, var, floor)))`
    /// without the temp-variable ceremony; see
    /// [`crate::constraint::SimilarTo`].
    pub fn similar_to(
        &self,
        probe: Value<EmbHandle>,
        var: Variable<EmbHandle>,
        score_floor: f32,
    ) -> crate::constraint::SimilarTo {
        let candidates = self
            .candidates_above(probe, score_floor)
            .map(|v| v.into_iter().map(|h| h.raw).collect())
            .unwrap_or_default();
        crate::constraint::SimilarTo::from_candidates(var, candidates)
    }

    /// Walk the graph from `from_handle`'s embedding and return
    /// every handle whose cosine similarity is at least
    /// `score_floor`. The core primitive the similarity
    /// constraint calls; exposed for tests and for callers who
    /// want the walk without the constraint wrapper.
    ///
    /// Bound by the view's `ef_search` (default 200) — callers
    /// pushing lots of above-threshold results need a wider
    /// beam via [`with_ef_search`][Self::with_ef_search].
    pub fn candidates_above(
        &self,
        from_handle: Value<EmbHandle>,
        score_floor: f32,
    ) -> Result<Vec<Value<EmbHandle>>, B::GetError<anybytes::view::ViewError>> {
        let Some(entry) = self.index.entry_point else {
            return Ok(Vec::new());
        };
        let from = self.cache.get(from_handle)?;
        let query: Vec<f32> = from.as_ref().as_ref().to_vec();
        if query.len() != self.index.dim {
            return Ok(Vec::new());
        }
        let mut curr = entry;
        for lvl in (1..=self.index.max_level).rev() {
            curr = self.greedy_search_layer(&query, curr, lvl)?;
        }
        let candidates = self.search_layer(&query, curr, self.ef_search, 0)?;
        Ok(candidates
            .into_iter()
            .filter(|(_, dist)| 1.0 - dist >= score_floor)
            .map(|(i, _)| {
                let raw = *self.index.handles.get(i as usize).expect("in range");
                Value::new(raw)
            })
            .collect())
    }

    fn dist_to(
        &self,
        q: &[f32],
        i: u32,
    ) -> Result<f32, B::GetError<anybytes::view::ViewError>> {
        let raw = *self.index.handles.get(i as usize).expect("in range");
        let handle: Value<EmbHandle> = Value::new(raw);
        let view = self.cache.get(handle)?;
        Ok(crate::hnsw::cosine_dist(q, view.as_ref().as_ref()))
    }

    fn greedy_search_layer(
        &self,
        q: &[f32],
        entry: u32,
        layer: u8,
    ) -> Result<u32, B::GetError<anybytes::view::ViewError>> {
        let mut curr = entry;
        let mut curr_dist = self.dist_to(q, curr)?;
        loop {
            let mut changed = false;
            let neigh: Vec<u32> = self
                .index
                .graph
                .neighbours(curr as usize, layer as usize)
                .collect();
            if neigh.is_empty() {
                return Ok(curr);
            }
            for n in neigh {
                let d = self.dist_to(q, n)?;
                if d < curr_dist {
                    curr_dist = d;
                    curr = n;
                    changed = true;
                }
            }
            if !changed {
                return Ok(curr);
            }
        }
    }

    fn search_layer(
        &self,
        q: &[f32],
        entry: u32,
        ef: usize,
        layer: u8,
    ) -> Result<Vec<(u32, f32)>, B::GetError<anybytes::view::ViewError>> {
        use std::collections::{BinaryHeap, HashSet};
        let mut visited: HashSet<u32> = HashSet::new();
        visited.insert(entry);
        let d0 = self.dist_to(q, entry)?;

        #[derive(Clone, Copy)]
        struct MinD {
            idx: u32,
            dist: f32,
        }
        impl PartialEq for MinD {
            fn eq(&self, o: &Self) -> bool {
                self.dist == o.dist
            }
        }
        impl Eq for MinD {}
        impl PartialOrd for MinD {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for MinD {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                o.dist
                    .partial_cmp(&self.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }
        #[derive(Clone, Copy)]
        struct MaxD {
            idx: u32,
            dist: f32,
        }
        impl PartialEq for MaxD {
            fn eq(&self, o: &Self) -> bool {
                self.dist == o.dist
            }
        }
        impl Eq for MaxD {}
        impl PartialOrd for MaxD {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        impl Ord for MaxD {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                self.dist
                    .partial_cmp(&o.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        let mut candidates: BinaryHeap<MinD> = BinaryHeap::new();
        candidates.push(MinD {
            idx: entry,
            dist: d0,
        });
        let mut results: BinaryHeap<MaxD> = BinaryHeap::new();
        results.push(MaxD {
            idx: entry,
            dist: d0,
        });
        while let Some(c) = candidates.pop() {
            let farthest = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
            if c.dist > farthest && results.len() >= ef {
                break;
            }
            let neigh: Vec<u32> = self
                .index
                .graph
                .neighbours(c.idx as usize, layer as usize)
                .collect();
            for n in neigh {
                if !visited.insert(n) {
                    continue;
                }
                let d = self.dist_to(q, n)?;
                let farthest = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
                if d < farthest || results.len() < ef {
                    candidates.push(MinD { idx: n, dist: d });
                    results.push(MaxD { idx: n, dist: d });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        Ok(results.into_iter().map(|m| (m.idx, m.dist)).collect())
    }
}

impl<'a, B> crate::constraint::SimilaritySearch for AttachedSuccinctHNSWIndex<'a, B>
where
    B: triblespace_core::repo::BlobStoreGet<Blake3>,
{
    fn neighbours_above(
        &self,
        from: Value<EmbHandle>,
        score_floor: f32,
    ) -> Vec<Value<EmbHandle>> {
        self.candidates_above(from, score_floor).unwrap_or_default()
    }

    fn cosine_between(
        &self,
        a: Value<EmbHandle>,
        b: Value<EmbHandle>,
    ) -> Option<f32> {
        let va = self.cache.get(a).ok()?;
        let vb = self.cache.get(b).ok()?;
        let a_slice: &[f32] = va.as_ref().as_ref();
        let b_slice: &[f32] = vb.as_ref().as_ref();
        if a_slice.len() != b_slice.len() {
            return None;
        }
        let mut sum = 0.0f32;
        for (x, y) in a_slice.iter().zip(b_slice.iter()) {
            sum += x * y;
        }
        Some(sum)
    }
}

/// Zero-copy, jerky-backed BM25 index.
///
/// Same query surface as [`crate::bm25::BM25Index`], but postings
/// and doc_lens live in bit-packed [`CompactVector`]s and the
/// doc-id / term tables are sliced directly out of an
/// [`anybytes::Bytes`] region without copying.
///
/// For 100k wiki fragments the naive blob is ~157 MiB; this
/// representation cuts it to ~86 MiB via bit-packed doc_idx
/// and u16-quantized scores. The quantization step is
/// internal — query-time scoring goes through
/// [`Self::score`], which returns the f32 sum derived from
/// the stored postings.
///
/// Built directly via
/// [`BM25Builder::build`][crate::bm25::BM25Builder::build]: the
/// builder sorts + dedups the keys into a `CompressedUniverse`
/// first, then accumulates tf and scores keyed by the universe
/// code from the start — no insertion-order intermediate. The
/// naive [`crate::bm25::BM25Index`] is kept as a pub reference
/// oracle only.
///
/// # Example
///
/// ```
/// use triblespace_core::id::Id;
/// use triblespace_search::bm25::BM25Builder;
/// use triblespace_search::succinct::SuccinctBM25Index;
/// use triblespace_search::tokens::hash_tokens;
///
/// let mut b = BM25Builder::new();
/// b.insert(&Id::new([1; 16]).unwrap(), hash_tokens("the quick brown fox"));
/// b.insert(&Id::new([2; 16]).unwrap(), hash_tokens("the lazy brown dog"));
/// b.insert(&Id::new([3; 16]).unwrap(), hash_tokens("quick silver fox"));
/// let idx = b.build();
///
/// // Same query API as BM25Index — "fox" hits two docs.
/// let fox = hash_tokens("fox")[0];
/// let hits: Vec<_> = idx.query_term(&fox).collect();
/// assert_eq!(hits.len(), 2);
///
/// // Persist via to_bytes / ToBlob<SuccinctBM25Blob> for pile storage.
/// let blob_bytes = idx.to_bytes();
/// assert!(blob_bytes.len() > 0);
/// ```
pub struct SuccinctBM25Index<
    D: ValueSchema = triblespace_core::value::schemas::genid::GenId,
    T: ValueSchema = crate::tokens::WordHash,
> {
    /// Sorted, deduplicated, compressed doc-key table. For
    /// entity-keyed corpora (`Value<GenId>`), 16 of the 32 bytes
    /// per key are always zero; plus real-world ID patterns
    /// share 4-byte fragments across docs. `CompressedUniverse`
    /// frequency-sorts fragments and stores indices via
    /// DACs-byte — typical 3-5× savings vs. flat `FixedBytesTable`.
    ///
    /// The doc_idx in the postings table is the key's position
    /// in the sorted universe (not insertion order).
    /// `keys.access(code)` decodes back to `RawValue`.
    keys: CompressedUniverse,
    doc_lens: SuccinctDocLens,
    terms: FixedBytesTable<32>,
    postings: SuccinctPostings,
    avg_doc_len: f32,
    k1: f32,
    b: f32,
    _phantom: std::marker::PhantomData<(D, T)>,
}

impl<D: ValueSchema, T: ValueSchema> std::fmt::Debug for SuccinctBM25Index<D, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuccinctBM25Index")
            .field("n_docs", &self.keys.len())
            .field("n_terms", &self.terms.len())
            .field("avg_doc_len", &self.avg_doc_len)
            .field("k1", &self.k1)
            .field("b", &self.b)
            .finish()
    }
}

impl<D: ValueSchema, T: ValueSchema> SuccinctBM25Index<D, T> {
    /// Direct-to-succinct builder path: consume a
    /// [`BM25Builder`][crate::bm25::BM25Builder] and produce the
    /// succinct index in a single pass through the docs.
    ///
    /// The universe is built first (sort + dedup keys into
    /// `CompressedUniverse` codes), then tf accumulation and
    /// scoring use those codes as the identifier from the start
    /// — no insertion-order → universe-code remap, no per-term
    /// resort pass. `BM25Builder::build()` delegates here.
    pub(crate) fn from_builder(
        builder: crate::bm25::BM25Builder<D, T>,
    ) -> Self {
        let crate::bm25::BM25Builder { docs, k1, b, _phantom: _ } = builder;

        // ── 1. keys: sort + dedup into CompressedUniverse. ─────────
        let (keys_bytes, keys_meta) = {
            let mut area = ByteArea::new().expect("alloc ByteArea");
            let mut sections = area.sections();
            let universe =
                CompressedUniverse::with(docs.iter().map(|(k, _)| *k), &mut sections);
            let meta = universe.metadata();
            let _ = universe;
            let _ = sections;
            (area.freeze().expect("freeze ByteArea"), meta)
        };
        let keys = CompressedUniverse::from_bytes(keys_meta, keys_bytes)
            .expect("round-trip the universe we just built");
        let n_universe = keys.len();

        // ── 2. tf accumulation keyed by universe_code from the
        // start; doc_lens indexed by code, last-write-wins for
        // duplicate keys (same semantics as the naive+remap
        // flow). ───────────────────────────────────────────────────
        let mut doc_lens_vec = vec![0u32; n_universe];
        let mut term_to_tfs: HashMap<RawValue, HashMap<u32, u32>> = HashMap::new();
        for (key, terms) in docs {
            let code = keys
                .search(&key)
                .expect("key just inserted into universe") as u32;
            doc_lens_vec[code as usize] = terms.len() as u32;
            for term in terms {
                *term_to_tfs.entry(term).or_default().entry(code).or_insert(0) += 1;
            }
        }
        let avg_doc_len = if n_universe == 0 {
            0.0
        } else {
            doc_lens_vec.iter().map(|&n| n as f64).sum::<f64>() as f32
                / n_universe as f32
        };

        // ── 3. doc_lens → succinct. ────────────────────────────────
        let (doc_lens_bytes, doc_lens_meta) =
            SuccinctDocLens::build(&doc_lens_vec).expect("build doc_lens");
        let doc_lens = SuccinctDocLens::from_bytes(doc_lens_meta, doc_lens_bytes)
            .expect("round-trip doc_lens");

        // ── 4. terms: sort ascending, pack flat. ──────────────────
        let mut term_rows: Vec<RawValue> = term_to_tfs.keys().copied().collect();
        term_rows.sort_unstable();
        let terms_bytes = FixedBytesTable::<32>::build(&term_rows);
        let terms = FixedBytesTable::<32>::from_bytes(terms_bytes, term_rows.len())
            .expect("round-trip terms table");

        // ── 5. per-term scored postings, each sorted ascending
        // by universe_code (required by SuccinctPostings).
        //
        // Streaming: SuccinctPostings::build_with materializes
        // one term's postings at a time into a reused buffer
        // instead of allocating a Vec<Vec<(u32, f32)>> for the
        // whole corpus. At 100 k docs / 300 k terms / Heaps-law
        // vocabulary that's the difference between ~400 KB peak
        // and ~144 MB peak for the postings staging area. ─────────
        let n = n_universe as f32;
        let (postings_bytes, postings_meta) = SuccinctPostings::build_with(
            n_universe as u32,
            term_rows.len(),
            |t, buf| {
                let tfs = &term_to_tfs[&term_rows[t]];
                let df = tfs.len() as f32;
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
                buf.extend(tfs.iter().map(|(&code, &tf)| {
                    let tf_f = tf as f32;
                    let dl = doc_lens_vec[code as usize] as f32;
                    let norm = if avg_doc_len > 0.0 {
                        1.0 - b + b * (dl / avg_doc_len)
                    } else {
                        1.0
                    };
                    let score = idf * (tf_f * (k1 + 1.0)) / (tf_f + k1 * norm);
                    (code, score)
                }));
                buf.sort_unstable_by_key(|&(code, _)| code);
            },
        )
        .expect("build postings");
        let postings = SuccinctPostings::from_bytes(postings_meta, postings_bytes)
            .expect("round-trip postings");

        Self {
            keys,
            doc_lens,
            terms,
            postings,
            avg_doc_len,
            k1,
            b,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Number of documents.
    pub fn doc_count(&self) -> usize {
        self.keys.len()
    }

    /// Number of distinct terms.
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Average document length used at build time.
    pub fn avg_doc_len(&self) -> f32 {
        self.avg_doc_len
    }

    /// BM25 `k1` used at build time.
    pub fn k1(&self) -> f32 {
        self.k1
    }

    /// BM25 `b` used at build time.
    pub fn b(&self) -> f32 {
        self.b
    }

    /// Length of doc `i`, or `None` if out of range.
    pub fn doc_len(&self, i: usize) -> Option<u32> {
        self.doc_lens.get(i)
    }

    /// Equality tolerance for bound-score matching, derived from
    /// the stored quantization scale. See
    /// [`SuccinctPostings::score_tolerance`].
    pub fn score_tolerance(&self) -> f32 {
        self.postings.score_tolerance()
    }

    /// Number of documents containing `term`.
    pub fn doc_frequency(&self, term: &Value<T>) -> usize {
        match self.terms.binary_search(&term.raw) {
            Ok(t) => self.postings.posting_count(t).unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Iterate `(Value<D>, f32)` postings for `term`. Empty if
    /// the term is absent.
    pub fn query_term<'a>(
        &'a self,
        term: &Value<T>,
    ) -> Box<dyn Iterator<Item = (Value<D>, f32)> + 'a> {
        match self.terms.binary_search(&term.raw) {
            Ok(t) => match self.postings.postings_for(t) {
                Some(iter) => Box::new(iter.map(move |(doc_idx, score)| {
                    let key = self.keys.access(doc_idx as usize);
                    (Value::<D>::new(key), score)
                })),
                None => Box::new(std::iter::empty()),
            },
            Err(_) => Box::new(std::iter::empty()),
        }
    }

    /// Score a multi-term query as the sum of per-term BM25
    /// weights (standard OR-like bag-of-words). Returned
    /// `(Value<D>, f32)` pairs are sorted descending by score;
    /// no top-k truncation — caller slices what they need.
    pub fn query_multi(&self, terms: &[Value<T>]) -> Vec<(Value<D>, f32)> {
        let mut acc: std::collections::HashMap<RawValue, f32> =
            std::collections::HashMap::new();
        for term in terms {
            for (key, score) in self.query_term(term) {
                *acc.entry(key.raw).or_insert(0.0) += score;
            }
        }
        let mut out: Vec<(Value<D>, f32)> =
            acc.into_iter().map(|(raw, s)| (Value::<D>::new(raw), s)).collect();
        out.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Serialize to a self-contained blob. The layout:
    ///
    /// ```text
    /// [header         ] SUCCINCT_HEADER_LEN B
    /// [keys           ] n_docs × 32 B
    /// [terms          ] n_terms × 32 B
    /// [doc_lens_bytes ] variable (CompactVector body)
    /// [postings_bytes ] variable (3 × CompactVector in one ByteArea:
    ///                            doc_idx + offsets + u16 scores)
    /// ```
    ///
    /// The header carries the scalar params, four
    /// `CompactVectorMeta` structures (doc_lens,
    /// postings.doc_idx, postings.offsets, postings.scores), the
    /// score quantization scale `max_score`, and 4 section
    /// (offset, length) pairs.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n_docs = self.doc_count() as u64;
        let n_terms = self.term_count() as u64;

        // keys: re-serialize through a fresh CompressedUniverse
        // so the embedded section handles match the freshly-
        // allocated ByteArea offsets.
        let keys_sorted: Vec<RawValue> =
            (0..self.doc_count()).map(|i| self.keys.access(i)).collect();
        let (keys_region, keys_meta) = {
            let mut area = ByteArea::new().expect("byte area");
            let mut sections = area.sections();
            let universe = CompressedUniverse::with_sorted_dedup(
                keys_sorted.into_iter(),
                &mut sections,
            );
            let meta = universe.metadata();
            let _ = universe;
            let _ = sections;
            (area.freeze().expect("freeze"), meta)
        };
        let keys_meta_on_disk: CompressedUniverseMetaOnDisk = keys_meta.into();

        // doc_lens: re-serialize so the blob owns the bytes
        // AND so the meta's handle offsets match the fresh
        // ByteArea (the stale in-memory meta points at the
        // original area).
        let (doc_lens_region, doc_lens_meta_fresh) =
            SuccinctDocLens::build(&self.doc_lens.to_vec()).expect("re-serialize");
        let doc_lens_meta: CompactVectorMetaOnDisk = doc_lens_meta_fresh.into();
        // postings: round-trip through build to get fresh bytes
        // + a matching meta (handle offsets re-relative to the
        // fresh ByteArea start). Streamed via build_with so the
        // re-serialization peak temp drops from ~144 MB at 100 k
        // docs to ~400 KB (one term's postings at a time).
        let (postings_region, postings_meta) = SuccinctPostings::build_with(
            self.doc_count() as u32,
            self.term_count(),
            |t, buf| {
                buf.extend(self.postings.postings_for(t).unwrap());
            },
        )
        .expect("re-serialize");
        let postings_doc_idx_meta: CompactVectorMetaOnDisk = postings_meta.doc_idx.into();
        let postings_offsets_meta: CompactVectorMetaOnDisk = postings_meta.offsets.into();
        let postings_scores_meta: CompactVectorMetaOnDisk = postings_meta.scores.into();

        // Round each section's start offset up to 8-byte
        // alignment. jerky's CompactVector / Universe views
        // reinterpret the bytes as u64 words and fail with an
        // `Alignment` error if the slice doesn't start on an
        // 8-byte boundary.
        fn align8(n: u64) -> u64 {
            (n + 7) & !7
        }
        let keys_off = 0u64;
        let keys_len = keys_region.len() as u64;
        let terms_off = align8(keys_off + keys_len);
        // Each term row is exactly 32 bytes (FixedBytesTable<32>),
        // so the section size is computable without materializing
        // a Vec<u8> just to read its `.len()`. Saves 9.6 MB at
        // 300 k terms / 100 k docs.
        let terms_len = self.term_count() as u64 * 32;
        let doc_lens_off = align8(terms_off + terms_len);
        let doc_lens_len = doc_lens_region.len() as u64;
        let postings_off = align8(doc_lens_off + doc_lens_len);
        let postings_len = postings_region.len() as u64;

        let body_len = postings_off + postings_len;
        let mut buf = Vec::with_capacity(SUCCINCT_HEADER_LEN + body_len as usize);

        // ── header (264 B) ────────────────────────────────────
        buf.extend_from_slice(&self.avg_doc_len.to_le_bytes()); // 4
        buf.extend_from_slice(&self.k1.to_le_bytes()); // 4
        buf.extend_from_slice(&self.b.to_le_bytes()); // 4
        buf.extend_from_slice(&postings_meta.max_score.to_le_bytes()); // 4
        buf.extend_from_slice(&n_docs.to_le_bytes()); // 8
        buf.extend_from_slice(&n_terms.to_le_bytes()); // 8
        buf.extend_from_slice(doc_lens_meta.as_bytes()); // 32
        buf.extend_from_slice(postings_doc_idx_meta.as_bytes()); // 32
        buf.extend_from_slice(postings_offsets_meta.as_bytes()); // 32
        buf.extend_from_slice(postings_scores_meta.as_bytes()); // 32
        buf.extend_from_slice(keys_meta_on_disk.as_bytes()); // 40
        buf.extend_from_slice(&keys_off.to_le_bytes()); // 8
        buf.extend_from_slice(&keys_len.to_le_bytes());
        buf.extend_from_slice(&terms_off.to_le_bytes());
        buf.extend_from_slice(&terms_len.to_le_bytes());
        buf.extend_from_slice(&doc_lens_off.to_le_bytes());
        buf.extend_from_slice(&doc_lens_len.to_le_bytes());
        buf.extend_from_slice(&postings_off.to_le_bytes());
        buf.extend_from_slice(&postings_len.to_le_bytes());
        debug_assert_eq!(buf.len(), SUCCINCT_HEADER_LEN);

        // ── body ──────────────────────────────────────────────
        // Pad to the aligned section offsets we computed above.
        let write_section = |buf: &mut Vec<u8>, target_off: u64, payload: &[u8]| {
            let target = SUCCINCT_HEADER_LEN + target_off as usize;
            if target > buf.len() {
                buf.resize(target, 0);
            }
            buf.extend_from_slice(payload);
        };
        write_section(&mut buf, keys_off, &keys_region);
        // Terms section: stream rows from FixedBytesTable directly
        // into the output buffer without an intermediate Vec<u8>.
        {
            let target = SUCCINCT_HEADER_LEN + terms_off as usize;
            if target > buf.len() {
                buf.resize(target, 0);
            }
            for i in 0..self.term_count() {
                let row = self.terms.get(i).copied().unwrap_or([0u8; 32]);
                buf.extend_from_slice(&row);
            }
        }
        write_section(&mut buf, doc_lens_off, &doc_lens_region);
        write_section(&mut buf, postings_off, &postings_region);
        buf
    }

    /// Reload from bytes previously produced by [`Self::to_bytes`].
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, SuccinctLoadError> {
        if bytes.len() < SUCCINCT_HEADER_LEN {
            return Err(SuccinctLoadError::ShortHeader);
        }
        let avg_doc_len = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let k1 = f32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let b = f32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let max_score = f32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let n_docs = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let n_terms = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;

        let doc_lens_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[32..64])
            .map_err(|_| SuccinctLoadError::BadMeta("doc_lens"))?
            .to_jerky();
        let postings_doc_idx_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[64..96])
            .map_err(|_| SuccinctLoadError::BadMeta("postings_doc_idx"))?
            .to_jerky();
        let postings_offsets_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[96..128])
            .map_err(|_| SuccinctLoadError::BadMeta("postings_offsets"))?
            .to_jerky();
        let postings_scores_meta = CompactVectorMetaOnDisk::read_from_bytes(&bytes[128..160])
            .map_err(|_| SuccinctLoadError::BadMeta("postings_scores"))?
            .to_jerky();
        let keys_meta = CompressedUniverseMetaOnDisk::read_from_bytes(&bytes[160..200])
            .map_err(|_| SuccinctLoadError::BadMeta("keys"))?
            .to_jerky();

        let read_u64 = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
        let keys_off = read_u64(200) as usize;
        let keys_len = read_u64(208) as usize;
        let terms_off = read_u64(216) as usize;
        let terms_len = read_u64(224) as usize;
        let doc_lens_off = read_u64(232) as usize;
        let doc_lens_len = read_u64(240) as usize;
        let postings_off = read_u64(248) as usize;
        let postings_len = read_u64(256) as usize;
        debug_assert_eq!(SUCCINCT_HEADER_LEN, 264);

        let body_start = SUCCINCT_HEADER_LEN;
        let body = &bytes[body_start..];
        let check = |end: usize, name: &'static str| -> Result<(), SuccinctLoadError> {
            if end > body.len() {
                Err(SuccinctLoadError::TruncatedSection(name))
            } else {
                Ok(())
            }
        };
        check(keys_off + keys_len, "keys")?;
        check(terms_off + terms_len, "terms")?;
        check(doc_lens_off + doc_lens_len, "doc_lens")?;
        check(postings_off + postings_len, "postings")?;

        let body_bytes = Bytes::from_source(body.to_vec());
        let keys_bytes = body_bytes.slice(keys_off..keys_off + keys_len);
        let terms_bytes = body_bytes.slice(terms_off..terms_off + terms_len);
        let doc_lens_bytes = body_bytes.slice(doc_lens_off..doc_lens_off + doc_lens_len);
        let postings_bytes = body_bytes.slice(postings_off..postings_off + postings_len);

        let keys = CompressedUniverse::from_bytes(keys_meta, keys_bytes)
            .map_err(|_| SuccinctLoadError::TruncatedSection("keys"))?;
        debug_assert_eq!(keys.len(), n_docs);
        let terms = FixedBytesTable::<32>::from_bytes(terms_bytes, n_terms)
            .map_err(|_| SuccinctLoadError::TruncatedSection("terms"))?;
        let doc_lens = SuccinctDocLens::from_bytes(doc_lens_meta, doc_lens_bytes)
            .map_err(|_| SuccinctLoadError::TruncatedSection("doc_lens"))?;
        let postings_meta = SuccinctPostingsMeta {
            doc_idx: postings_doc_idx_meta,
            offsets: postings_offsets_meta,
            scores: postings_scores_meta,
            max_score,
            n_terms: n_terms as u64,
        };
        let postings = SuccinctPostings::from_bytes(postings_meta, postings_bytes)
            .map_err(|_| SuccinctLoadError::TruncatedSection("postings"))?;

        Ok(Self {
            keys,
            doc_lens,
            terms,
            postings,
            avg_doc_len,
            k1,
            b,
            _phantom: std::marker::PhantomData,
        })
    }
}


/// Header length in bytes for an `SuccinctBM25Index` blob.
///
/// Layout: 4 avg_doc_len, 4 k1, 4 b, 4 max_score, 8 n_docs,
/// 8 n_terms, 4×32 CompactVectorMeta (doc_lens,
/// postings.doc_idx, postings.offsets, postings.scores), 1×40
/// CompressedUniverseMeta (keys), 4×16 section (offset, len)
/// = 264.
///
/// No magic, no version field — a typed `BlobSchema` handle on
/// the pile side carries the identity. Breaking format changes
/// mint a new schema id.
///
/// 264 is a multiple of 8, so `body_offset = SUCCINCT_HEADER_LEN
/// + section_offset` lands on a u64-aligned absolute address —
/// jerky's view types reinterpret the slice as `[u64]` and
/// refuse misaligned starts with an `Alignment` error.
const SUCCINCT_HEADER_LEN: usize = 264;

/// Errors loading a `SuccinctBM25Index` or `SuccinctHNSWIndex`
/// blob.
#[derive(Debug, Clone, PartialEq)]
pub enum SuccinctLoadError {
    /// Blob shorter than the fixed header.
    ShortHeader,
    /// A declared section extends past the blob body.
    TruncatedSection(&'static str),
    /// A `CompactVectorMeta` in the header couldn't be parsed.
    BadMeta(&'static str),
}

impl std::fmt::Display for SuccinctLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShortHeader => write!(f, "succinct blob shorter than header"),
            Self::TruncatedSection(name) => {
                write!(f, "succinct blob: truncated section `{name}`")
            }
            Self::BadMeta(name) => write!(f, "succinct blob: bad meta `{name}`"),
        }
    }
}

impl std::error::Error for SuccinctLoadError {}

/// Content-addressed [`BlobSchema`] marker for the succinct
/// BM25 blob format — 264 B header + bit-packed body.
///
/// Schema id minted fresh via `trible genid`:
/// `5A1EF3FFD638B15E3EBEAA1E92660441`. Any breaking format
/// change mints a new id (i.e. a new type), which the
/// compiler treats as a different blob entirely — so readers
/// can't accidentally deserialize a mismatched layout.
///
/// Previous id `68C03764D04D05DF65E49589FBBA1441` retired
/// when the blob dropped its magic+version preamble and the
/// now-unnecessary reserved u32, so it's not drop-in
/// compatible with older blobs.
pub enum SuccinctBM25Blob {}

impl ConstId for SuccinctBM25Blob {
    const ID: Id = id_hex!("5A1EF3FFD638B15E3EBEAA1E92660441");
}

impl BlobSchema for SuccinctBM25Blob {}

// Default `describe` — fragment rooted at `Self::ID` with an
// empty TribleSet. Lets `attributes!` declare value types like
// `Handle<Blake3, SuccinctBM25Blob>` without the macro complaining
// that the schema can't describe itself.
impl ConstDescribe for SuccinctBM25Blob {}

impl<D: ValueSchema, T: ValueSchema> ToBlob<SuccinctBM25Blob> for &SuccinctBM25Index<D, T> {
    fn to_blob(self) -> Blob<SuccinctBM25Blob> {
        Blob::new(Bytes::from_source(self.to_bytes()))
    }
}

impl<D: ValueSchema, T: ValueSchema> ToBlob<SuccinctBM25Blob> for SuccinctBM25Index<D, T> {
    fn to_blob(self) -> Blob<SuccinctBM25Blob> {
        (&self).to_blob()
    }
}

impl<D: ValueSchema, T: ValueSchema> TryFromBlob<SuccinctBM25Blob> for SuccinctBM25Index<D, T> {
    type Error = SuccinctLoadError;

    fn try_from_blob(blob: Blob<SuccinctBM25Blob>) -> Result<Self, Self::Error> {
        SuccinctBM25Index::try_from_bytes(blob.bytes.as_ref())
    }
}

/// Header length in bytes for a `SuccinctHNSWIndex` blob.
///
/// Layout: 4 dim + 2 m + 2 m0 + 1 max_level + 1 reserved +
/// 1 has_entry + 1 reserved + 4 entry_point + 8 n_nodes +
/// 8 n_layers + 2 × 32 CompactVectorMeta + 4 × 8 section
/// (offset, len) = 128.
///
/// No magic, no version — schema identity is carried by the
/// typed `BlobSchema` handle. Breaking format changes mint a
/// new schema id.
const SH25_HEADER_LEN: usize = 128;

/// Content-addressed [`BlobSchema`] marker for the succinct
/// HNSW blob format — 128 B header + handles + jerky-packed
/// graph. Embeddings themselves live as separate blobs in the
/// pile, referenced by handle.
///
/// Schema id minted via `trible genid`:
/// `A96890DE5F85A4F2285C365549B21BC2`. Any breaking format
/// change mints a new id (i.e. a new type), so the compiler
/// rules out mismatched-layout deserialization.
///
/// Retired IDs (bytes in the wild under these tags have the
/// old layouts and can't be loaded by the current code):
///
/// - `27D71A473EF22DA4D916F61810AC5D86` — carried a keys
///   section alongside handles (schema-tagged doc keys). The
///   keys table was redundant with the caller's own
///   doc↔handle tribles, so the split was dropped.
/// - `7AFE59E7F895B23F05452FF7919E12E4` — pre-magic/version
///   rotation.
pub enum SuccinctHNSWBlob {}

impl ConstId for SuccinctHNSWBlob {
    const ID: Id = id_hex!("A96890DE5F85A4F2285C365549B21BC2");
}

impl BlobSchema for SuccinctHNSWBlob {}

impl ConstDescribe for SuccinctHNSWBlob {}

impl ToBlob<SuccinctHNSWBlob> for &SuccinctHNSWIndex {
    fn to_blob(self) -> Blob<SuccinctHNSWBlob> {
        Blob::new(Bytes::from_source(self.to_bytes()))
    }
}

impl ToBlob<SuccinctHNSWBlob> for SuccinctHNSWIndex {
    fn to_blob(self) -> Blob<SuccinctHNSWBlob> {
        (&self).to_blob()
    }
}

impl TryFromBlob<SuccinctHNSWBlob> for SuccinctHNSWIndex {
    type Error = SuccinctLoadError;

    fn try_from_blob(blob: Blob<SuccinctHNSWBlob>) -> Result<Self, Self::Error> {
        SuccinctHNSWIndex::try_from_bytes(blob.bytes.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::repo::BlobStore;

    #[test]
    fn empty_roundtrip() {
        let (bytes, meta) = SuccinctDocLens::build(&[]).unwrap();
        let view = SuccinctDocLens::from_bytes(meta, bytes).unwrap();
        assert!(view.is_empty());
        assert_eq!(view.len(), 0);
        assert_eq!(view.get(0), None);
    }

    #[test]
    fn small_roundtrip() {
        let lens = vec![3u32, 0, 7, 1, 15];
        let (bytes, meta) = SuccinctDocLens::build(&lens).unwrap();
        let view = SuccinctDocLens::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.len(), lens.len());
        for (i, &n) in lens.iter().enumerate() {
            assert_eq!(view.get(i), Some(n), "mismatch at {i}");
        }
        assert_eq!(view.to_vec(), lens);
    }

    #[test]
    fn out_of_range_is_none() {
        let (bytes, meta) = SuccinctDocLens::build(&[1u32, 2, 3]).unwrap();
        let view = SuccinctDocLens::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.get(3), None);
        assert_eq!(view.get(99), None);
    }

    #[test]
    fn width_matches_max_value() {
        // max 15 -> 4 bits, max 16 -> 5 bits.
        assert_eq!(required_width(&[0, 15, 7, 3]), 4);
        assert_eq!(required_width(&[0, 16]), 5);
        // all zeros -> width 1 (CompactVector min).
        assert_eq!(required_width(&[0, 0, 0]), 1);
        // empty -> width 1.
        assert_eq!(required_width(&[]), 1);
    }

    #[test]
    fn large_lens_pack_correctly() {
        // Lengths up to 1_000_000 — 20 bits per entry. Round-trip
        // must preserve the full range.
        let lens: Vec<u32> = (0..200).map(|i| i * 5_000).collect();
        let (bytes, meta) = SuccinctDocLens::build(&lens).unwrap();
        let view = SuccinctDocLens::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.to_vec(), lens);
        assert_eq!(view.width(), 20); // log2(995_000) rounded up.
    }

    #[test]
    fn bit_packing_beats_raw_u32() {
        // For a corpus where all docs are ≤255 tokens, each entry
        // packs into 8 bits instead of 32 — 4x smaller. Not an
        // exact assertion on bytes (CompactVector has small fixed
        // overhead), but the frozen bytes should be clearly
        // smaller than 4 * n.
        let lens: Vec<u32> = (0..1000).map(|i| (i % 200) as u32).collect();
        let (bytes, _meta) = SuccinctDocLens::build(&lens).unwrap();
        assert!(
            bytes.len() < lens.len() * 4,
            "succinct {} < naive {}",
            bytes.len(),
            lens.len() * 4
        );
    }

    #[test]
    fn fixed_table_16_roundtrip() {
        let rows = vec![[7u8; 16], [3u8; 16], [99u8; 16]];
        let bytes = FixedBytesTable::<16>::build(&rows);
        let view = FixedBytesTable::<16>::from_bytes(bytes, rows.len()).unwrap();
        assert_eq!(view.len(), 3);
        assert_eq!(view.get(0), Some(&[7u8; 16]));
        assert_eq!(view.get(2), Some(&[99u8; 16]));
        assert_eq!(view.get(3), None);
    }

    #[test]
    fn fixed_table_32_roundtrip() {
        let rows = vec![[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]];
        let bytes = FixedBytesTable::<32>::build(&rows);
        let view = FixedBytesTable::<32>::from_bytes(bytes, rows.len()).unwrap();
        assert_eq!(view.to_vec(), rows);
    }

    #[test]
    fn fixed_table_binary_search_finds_term() {
        // Sorted table of 32-byte terms — what BM25's term table
        // wants.
        let mut rows = vec![[5u8; 32], [1u8; 32], [9u8; 32], [3u8; 32]];
        rows.sort();
        let bytes = FixedBytesTable::<32>::build(&rows);
        let view = FixedBytesTable::<32>::from_bytes(bytes, rows.len()).unwrap();
        assert_eq!(view.binary_search(&[3u8; 32]), Ok(1));
        assert_eq!(view.binary_search(&[5u8; 32]), Ok(2));
        assert_eq!(view.binary_search(&[7u8; 32]), Err(3));
    }

    #[test]
    fn fixed_table_rejects_size_mismatch() {
        // 3 rows × 16 bytes = 48 bytes; claim 4 rows -> error.
        let rows = vec![[0u8; 16], [1u8; 16], [2u8; 16]];
        let bytes = FixedBytesTable::<16>::build(&rows);
        let err = FixedBytesTable::<16>::from_bytes(bytes, 4).unwrap_err();
        assert!(matches!(err, SuccinctDocLensError::SizeMismatch { .. }));
    }

    #[test]
    fn fixed_table_empty_is_empty() {
        let bytes = FixedBytesTable::<16>::build(&[]);
        let view = FixedBytesTable::<16>::from_bytes(bytes, 0).unwrap();
        assert!(view.is_empty());
        assert_eq!(view.get(0), None);
        assert_eq!(view.binary_search(&[0u8; 16]), Err(0));
    }

    #[test]
    fn postings_roundtrip_simple() {
        let lists = vec![
            vec![(0u32, 1.5f32), (3, 0.75), (7, 2.0)],
            vec![(1, 0.5), (2, 3.25)],
            vec![],
            vec![(4, 9.0)],
        ];
        let (bytes, meta) = SuccinctPostings::build(&lists, 8).unwrap();
        let view = SuccinctPostings::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.term_count(), 4);
        assert_eq!(view.posting_count(0), Some(3));
        assert_eq!(view.posting_count(1), Some(2));
        assert_eq!(view.posting_count(2), Some(0));
        assert_eq!(view.posting_count(3), Some(1));
        assert_eq!(view.posting_count(4), None);

        // Quantization is lossy: doc_idx round-trips exactly,
        // scores match within one bucket (= max_score / 65534).
        let tol = view.score_tolerance();
        for (t, expected) in lists.iter().enumerate() {
            let got: Vec<(u32, f32)> = view.postings_for(t).unwrap().collect();
            assert_eq!(got.len(), expected.len(), "term {t} length");
            for ((gd, gs), (ed, es)) in got.iter().zip(expected.iter()) {
                assert_eq!(gd, ed, "term {t} doc idx");
                assert!(
                    (gs - es).abs() <= tol,
                    "term {t} score drift {gs} vs {es} exceeds tol {tol}"
                );
            }
        }
    }

    #[test]
    fn postings_empty_corpus() {
        let (bytes, meta) = SuccinctPostings::build(&[] as &[Vec<(u32, f32)>], 0).unwrap();
        let view = SuccinctPostings::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.term_count(), 0);
        assert!(view.postings_for(0).is_none());
    }

    #[test]
    fn build_with_streaming_matches_lists_build() {
        // Same fixture as `postings_roundtrip_simple`.
        let lists = vec![
            vec![(0u32, 1.5f32), (3, 0.75), (7, 2.0)],
            vec![(1, 0.5), (2, 3.25)],
            vec![],
            vec![(4, 9.0)],
        ];
        let (bytes_a, meta_a) = SuccinctPostings::build(&lists, 8).unwrap();
        // Streaming closure: caller materializes one term at a
        // time. We invoke the same path build() routes through,
        // but assert byte-for-byte equality with the lists path
        // to lock the determinism contract.
        let (bytes_b, meta_b) = SuccinctPostings::build_with(8, lists.len(), |t, buf| {
            buf.extend_from_slice(&lists[t]);
        })
        .unwrap();
        assert_eq!(bytes_a.as_ref(), bytes_b.as_ref(), "byte-identical output");
        assert_eq!(meta_a.max_score, meta_b.max_score);
        assert_eq!(meta_a.n_terms, meta_b.n_terms);
    }

    #[test]
    fn succinct_bm25_matches_naive_on_sample() {
        use crate::bm25::BM25Builder;
        use crate::tokens::hash_tokens;
        use triblespace_core::id::Id;

        fn iid(byte: u8) -> Id {
            Id::new([byte; 16]).unwrap()
        }

        let mut b = BM25Builder::new();
        b.insert(iid(1), hash_tokens("the quick brown fox"));
        b.insert(iid(2), hash_tokens("the lazy brown dog"));
        b.insert(iid(3), hash_tokens("quick silver fox jumps"));
        b.insert(iid(4), hash_tokens("unrelated filler content"));
        let naive = b.clone().build_naive();
        let succinct = b.build();

        assert_eq!(succinct.doc_count(), naive.doc_count());
        assert_eq!(succinct.term_count(), naive.term_count());
        assert_eq!(succinct.k1(), naive.k1());
        assert_eq!(succinct.b(), naive.b());
        assert!((succinct.avg_doc_len() - naive.avg_doc_len()).abs() < 1e-6);

        // Every stored term must produce matching postings. Scores
        // match within the succinct index's quantization tolerance.
        let tol = succinct.score_tolerance();
        for term_raw in naive.terms_slice() {
            let term: Value<crate::tokens::WordHash> = Value::new(*term_raw);
            let n: Vec<_> = naive.query_term(&term).collect();
            let s: Vec<_> = succinct.query_term(&term).collect();
            assert_eq!(
                n.len(),
                s.len(),
                "posting count mismatch for term {term_raw:x?}"
            );
            for ((n_id, n_s), (s_id, s_s)) in n.iter().zip(s.iter()) {
                assert_eq!(n_id.raw, s_id.raw);
                assert!(
                    (n_s - s_s).abs() <= tol,
                    "score drift for {n_id:?}: naive={n_s} succinct={s_s} > tol {tol}"
                );
            }
            assert_eq!(naive.doc_frequency(&term), succinct.doc_frequency(&term));
        }

        // Missing term returns nothing.
        let missing = hash_tokens("banana");
        assert!(succinct.query_term(&missing[0]).next().is_none());
        assert_eq!(succinct.doc_frequency(&missing[0]), 0);
    }

    #[test]
    fn succinct_bm25_empty_corpus() {
        use crate::bm25::BM25Builder;
        use triblespace_core::value::schemas::genid::GenId;
        let succinct = BM25Builder::<GenId, crate::tokens::WordHash>::new().build();
        assert_eq!(succinct.doc_count(), 0);
        assert_eq!(succinct.term_count(), 0);
        let probe: Value<crate::tokens::WordHash> = Value::new([0u8; 32]);
        assert!(succinct.query_term(&probe).next().is_none());
    }

    #[test]
    fn succinct_bm25_query_multi_matches_naive() {
        // Multi-term aggregate ranking must agree with the naive
        // implementation (within the quantization tolerance).
        // Use docs of DIFFERENT lengths so matching docs produce
        // distinct BM25 scores — otherwise tied docs can come
        // out in either order and the comparison flaps.
        use crate::bm25::BM25Builder;
        use crate::tokens::hash_tokens;
        use triblespace_core::id::Id;
        fn iid(byte: u8) -> Id {
            Id::new([byte; 16]).unwrap()
        }
        let mut b = BM25Builder::new();
        b.insert(iid(1), hash_tokens("quick fox"));
        b.insert(iid(2),
            hash_tokens("quick red rapid fox jumps high over fences"),
        );
        b.insert(iid(3), hash_tokens("slow brown dog"));
        let naive = b.clone().build_naive();
        let succinct = b.build();

        let q = hash_tokens("quick fox");
        let a = naive.query_multi(&q);
        let b = succinct.query_multi(&q);

        assert_eq!(a.len(), b.len());
        // Distinct scores → ranking is deterministic, so a.i = b.i.
        let tol = succinct.score_tolerance() * 2.0; // two terms summed.
        for ((a_id, a_s), (b_id, b_s)) in a.iter().zip(b.iter()) {
            assert_eq!(a_id, b_id, "ranking order mismatch");
            assert!(
                (a_s - b_s).abs() <= tol,
                "score drift: naive={a_s} succinct={b_s} > tol {tol}"
            );
        }
        assert_eq!(b.len(), 2);
    }

    fn build_succinct_sample() -> SuccinctBM25Index {
        use crate::bm25::BM25Builder;
        use crate::tokens::hash_tokens;
        use triblespace_core::id::Id;
        fn iid(byte: u8) -> Id {
            Id::new([byte; 16]).unwrap()
        }
        let mut b = BM25Builder::new().k1(1.4).b(0.72);
        b.insert(iid(1), hash_tokens("the quick brown fox"));
        b.insert(iid(2), hash_tokens("the lazy brown dog"));
        b.insert(iid(3), hash_tokens("quick silver fox jumps"));
        b.insert(iid(4), hash_tokens("completely unrelated filler content"));
        b.build()
    }

    #[test]
    fn succinct_bm25_bytes_round_trip() {
        use crate::tokens::hash_tokens;
        let original = build_succinct_sample();
        let bytes = original.to_bytes();
        let reloaded = SuccinctBM25Index::try_from_bytes(&bytes).expect("valid blob");

        assert_eq!(reloaded.doc_count(), original.doc_count());
        assert_eq!(reloaded.term_count(), original.term_count());
        assert_eq!(reloaded.k1(), original.k1());
        assert_eq!(reloaded.b(), original.b());
        assert!((reloaded.avg_doc_len() - original.avg_doc_len()).abs() < 1e-6);

        // Every term's posting list must round-trip. Both
        // copies were quantized with the same scale so buckets
        // are deterministic — scores match within a single ULP,
        // well under the quantization tolerance.
        let tol = original.score_tolerance().max(1e-5);
        for word in ["the", "fox", "quick", "brown", "dog"] {
            let term = hash_tokens(word)[0];
            let a: Vec<_> = original.query_term(&term).collect();
            let b: Vec<_> = reloaded.query_term(&term).collect();
            assert_eq!(a.len(), b.len(), "term '{word}' count mismatch");
            for ((a_id, a_s), (b_id, b_s)) in a.iter().zip(b.iter()) {
                assert_eq!(a_id, b_id);
                assert!(
                    (a_s - b_s).abs() <= tol,
                    "term '{word}': score drift {a_s} vs {b_s} > tol {tol}"
                );
            }
        }
    }

    #[test]
    fn succinct_bm25_empty_round_trip() {
        use crate::bm25::BM25Builder;
        use triblespace_core::value::schemas::genid::GenId;
        let idx = BM25Builder::<GenId, crate::tokens::WordHash>::new().build();
        let bytes = idx.to_bytes();
        let reloaded: SuccinctBM25Index =
            SuccinctBM25Index::try_from_bytes(&bytes).expect("valid blob");
        assert_eq!(reloaded.doc_count(), 0);
        assert_eq!(reloaded.term_count(), 0);
    }

    #[test]
    fn succinct_bm25_rejects_short_header() {
        let err = SuccinctBM25Index::<
            triblespace_core::value::schemas::genid::GenId,
            crate::tokens::WordHash,
        >::try_from_bytes(&[0u8; 10])
        .unwrap_err();
        assert_eq!(err, SuccinctLoadError::ShortHeader);
    }

    // Magic/version rejection tests retired along with the
    // fields themselves — the typed `BlobSchema` handle
    // carries blob identity now, and a breaking layout change
    // mints a new schema id (a different type).

    #[test]
    fn succinct_bm25_rejects_truncation() {
        let bytes = build_succinct_sample().to_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        let err = SuccinctBM25Index::<
            triblespace_core::value::schemas::genid::GenId,
            crate::tokens::WordHash,
        >::try_from_bytes(truncated)
        .unwrap_err();
        assert!(matches!(err, SuccinctLoadError::TruncatedSection(_)));
    }

    #[test]
    fn succinct_bm25_blob_schema_round_trip() {
        use triblespace_core::blob::{ToBlob, TryFromBlob};
        let original = build_succinct_sample();
        let blob: triblespace_core::blob::Blob<SuccinctBM25Blob> = (&original).to_blob();
        let reloaded: SuccinctBM25Index =
            SuccinctBM25Index::try_from_blob(blob).expect("valid blob");
        assert_eq!(reloaded.doc_count(), original.doc_count());
        assert_eq!(reloaded.term_count(), original.term_count());
    }

    #[test]
    fn succinct_bm25_blob_is_deterministic() {
        // Content-addressing guarantee: same corpus must produce
        // identical bytes across runs.
        let a = build_succinct_sample().to_bytes();
        let b = build_succinct_sample().to_bytes();
        assert_eq!(a, b);
    }

    #[test]
    fn graph_roundtrip_simple() {
        // 4 nodes, 2 layers.
        // Layer 0 (full graph): each node knows its two neighbours.
        // Layer 1: only 3 nodes participate; one has empty list.
        let layers = vec![
            vec![vec![1u32, 2], vec![0, 3], vec![0, 3], vec![1, 2]],
            vec![
                vec![2u32],
                vec![], // node 1 absent → empty list
                vec![0],
                vec![], // node 3 absent → empty list
            ],
        ];
        let (bytes, meta) = SuccinctGraph::build(&layers, 4).unwrap();
        let view = SuccinctGraph::from_bytes(meta, bytes).unwrap();

        assert_eq!(view.n_nodes(), 4);
        assert_eq!(view.n_layers(), 2);

        for (layer_idx, layer) in layers.iter().enumerate() {
            for (i, expected) in layer.iter().enumerate() {
                let got: Vec<u32> = view.neighbours(i, layer_idx).collect();
                assert_eq!(&got, expected, "mismatch at (node {i}, layer {layer_idx})");
            }
        }
    }

    #[test]
    fn graph_out_of_range() {
        let layers = vec![vec![vec![1u32], vec![0]]];
        let (bytes, meta) = SuccinctGraph::build(&layers, 2).unwrap();
        let view = SuccinctGraph::from_bytes(meta, bytes).unwrap();
        assert!(view.neighbours(5, 0).next().is_none());
        assert!(view.neighbours(0, 99).next().is_none());
    }

    #[test]
    fn graph_empty() {
        let layers: Vec<Vec<Vec<u32>>> = vec![];
        let (bytes, meta) = SuccinctGraph::build(&layers, 0).unwrap();
        let view = SuccinctGraph::from_bytes(meta, bytes).unwrap();
        assert_eq!(view.n_nodes(), 0);
        assert_eq!(view.n_layers(), 0);
    }

    #[test]
    fn graph_rejects_out_of_range_neighbour() {
        // Neighbour refers to node 5 but corpus has only 3 nodes.
        let layers = vec![vec![vec![5u32], vec![0], vec![0]]];
        let err = SuccinctGraph::build(&layers, 3).unwrap_err();
        assert!(matches!(err, SuccinctDocLensError::SizeMismatch { .. }));
    }

    #[test]
    fn succinct_hnsw_matches_naive_on_sample() {
        use crate::hnsw::HNSWBuilder;
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::repo::BlobStore;
        use triblespace_core::value::schemas::hash::Blake3;

        // Small deterministic corpus of 4-D vectors. with_seed
        // locks the level sampling so the graph is reproducible.
        let mut store = MemoryBlobStore::<Blake3>::new();
        let mut b = HNSWBuilder::new(4).with_seed(42);
        let mut handles = Vec::new();
        for i in 1..=16u8 {
            let f = i as f32;
            let vec = vec![f.sin(), f.cos(), (f * 0.5).sin(), (f * 0.3).cos()];
            let h = crate::schemas::put_embedding::<_, Blake3>(&mut store, vec.clone()).unwrap();
            b.insert(h, vec).unwrap();
            handles.push(h);
        }
        let naive = b.build_naive();
        let succinct = SuccinctHNSWIndex::from_naive(&naive).unwrap();
        let reader = store.reader().unwrap();

        assert_eq!(succinct.doc_count(), naive.doc_count());
        assert_eq!(succinct.dim(), naive.dim());
        assert_eq!(succinct.max_level(), naive.max_level());

        // Query from a few probe handles and check the
        // above-threshold set matches exactly between naive and
        // succinct backends.
        let naive_view = naive.attach(&reader);
        let succinct_view = succinct.attach(&reader);
        let floor = 0.5f32;
        for probe in handles.iter().take(3) {
            let n: std::collections::HashSet<_> =
                naive_view.candidates_above(*probe, floor).unwrap().into_iter().collect();
            let s: std::collections::HashSet<_> =
                succinct_view.candidates_above(*probe, floor).unwrap().into_iter().collect();
            assert_eq!(n, s, "mismatch for probe {probe:?}");
        }
    }

    fn build_succinct_hnsw_sample() -> (
        SuccinctHNSWIndex,
        triblespace_core::blob::MemoryBlobStore<
            triblespace_core::value::schemas::hash::Blake3,
        >,
        Vec<
            triblespace_core::value::Value<
                triblespace_core::value::schemas::hash::Handle<
                    triblespace_core::value::schemas::hash::Blake3,
                    crate::schemas::Embedding,
                >,
            >,
        >,
    ) {
        use crate::hnsw::HNSWBuilder;
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::value::schemas::hash::Blake3;
        let mut store = MemoryBlobStore::<Blake3>::new();
        let mut b = HNSWBuilder::new(4).with_seed(17);
        let mut handles = Vec::new();
        for i in 1..=20u8 {
            let f = i as f32;
            let v = vec![f.sin(), f.cos(), (f * 0.7).sin(), (f * 0.3).cos()];
            let h = crate::schemas::put_embedding::<_, Blake3>(&mut store, v.clone()).unwrap();
            b.insert(h, v).unwrap();
            handles.push(h);
        }
        let idx = b.build();
        (idx, store, handles)
    }

    #[test]
    fn succinct_hnsw_bytes_round_trip() {
        let (original, mut store, handles) = build_succinct_hnsw_sample();
        let bytes = original.to_bytes();
        let reloaded = SuccinctHNSWIndex::try_from_bytes(&bytes).expect("valid blob");
        assert_eq!(reloaded.doc_count(), original.doc_count());
        assert_eq!(reloaded.dim(), original.dim());
        assert_eq!(reloaded.m(), original.m());
        assert_eq!(reloaded.m0(), original.m0());
        assert_eq!(reloaded.max_level(), original.max_level());

        // Same probe handle must return identical above-threshold
        // sets on both backends.
        let reader = store.reader().unwrap();
        let orig_hits: std::collections::HashSet<_> = original
            .attach(&reader)
            .candidates_above(handles[0], 0.5)
            .unwrap()
            .into_iter()
            .collect();
        let load_hits: std::collections::HashSet<_> = reloaded
            .attach(&reader)
            .candidates_above(handles[0], 0.5)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(orig_hits, load_hits);
    }

    #[test]
    fn succinct_hnsw_empty_round_trip() {
        use crate::hnsw::HNSWBuilder;
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::repo::BlobStore;
        use triblespace_core::value::schemas::hash::Blake3;
        let idx = HNSWBuilder::new(3).build();
        let bytes = idx.to_bytes();
        let reloaded: SuccinctHNSWIndex =
            SuccinctHNSWIndex::try_from_bytes(&bytes).expect("valid blob");
        assert_eq!(reloaded.doc_count(), 0);
        let mut store: MemoryBlobStore<Blake3> = MemoryBlobStore::new();
        let probe = crate::schemas::put_embedding::<_, Blake3>(
            &mut store,
            vec![1.0, 0.0, 0.0],
        )
        .unwrap();
        assert!(reloaded
            .attach(&store.reader().unwrap())
            .candidates_above(probe, 0.0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn succinct_hnsw_rejects_short_header() {
        let err = SuccinctHNSWIndex::try_from_bytes(&[0u8; 10]).unwrap_err();
        assert_eq!(err, SuccinctLoadError::ShortHeader);
    }

    // Magic/version rejection tests retired — `SuccinctHNSWBlob`
    // (the schema) is what a typed handle commits to; old-layout
    // blobs are literally a different type and can't reach this
    // reader.

    #[test]
    fn succinct_hnsw_rejects_truncation() {
        let (idx, _, _) = build_succinct_hnsw_sample();
        let bytes = idx.to_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        let err = SuccinctHNSWIndex::try_from_bytes(truncated).unwrap_err();
        assert!(matches!(err, SuccinctLoadError::TruncatedSection(_)));
    }

    #[test]
    fn succinct_hnsw_blob_schema_round_trip() {
        use triblespace_core::blob::{ToBlob, TryFromBlob};
        let (original, _, _) = build_succinct_hnsw_sample();
        let blob: triblespace_core::blob::Blob<SuccinctHNSWBlob> = (&original).to_blob();
        let reloaded: SuccinctHNSWIndex =
            SuccinctHNSWIndex::try_from_blob(blob).expect("valid blob");
        assert_eq!(reloaded.doc_count(), original.doc_count());
        assert_eq!(reloaded.dim(), original.dim());
    }

    #[test]
    fn succinct_hnsw_blob_is_deterministic() {
        let (a, _, _) = build_succinct_hnsw_sample();
        let (b, _, _) = build_succinct_hnsw_sample();
        assert_eq!(a.to_bytes(), b.to_bytes());
    }

    #[test]
    fn succinct_hnsw_empty_index() {
        use crate::hnsw::HNSWBuilder;
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::repo::BlobStore;
        use triblespace_core::value::schemas::hash::Blake3;
        let succinct = HNSWBuilder::new(3).build();
        assert_eq!(succinct.doc_count(), 0);
        let mut store: MemoryBlobStore<Blake3> = MemoryBlobStore::new();
        let probe = crate::schemas::put_embedding::<_, Blake3>(
            &mut store,
            vec![1.0, 0.0, 0.0],
        )
        .unwrap();
        assert!(succinct
            .attach(&store.reader().unwrap())
            .candidates_above(probe, 0.0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn graph_rejects_mismatched_layer_width() {
        // Layer has 2 node entries but n_nodes = 3.
        let layers = vec![vec![vec![1u32], vec![0]]];
        let err = SuccinctGraph::build(&layers, 3).unwrap_err();
        assert!(matches!(err, SuccinctDocLensError::SizeMismatch { .. }));
    }

    #[test]
    fn succinct_blob_smaller_than_naive_at_scale() {
        // Same corpus through naive and succinct paths — succinct
        // should fit in fewer bytes once postings bit-pack.
        use crate::bm25::BM25Builder;
        use crate::tokens::hash_tokens;
        use triblespace_core::id::Id;

        // Build a corpus large enough that the bit-packing wins
        // dominate the per-blob fixed overhead (~212B header +
        // metas).
        let mut b = BM25Builder::new();
        for i in 1..=250u16 {
            let text = format!("doc {i} contains the quick brown fox {}", i % 17);
            let id = Id::new([
                (i >> 8) as u8,
                (i & 0xff) as u8,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                0xaa,
                if i == 0 { 1 } else { 0xaa },
            ])
            .unwrap();
            b.insert(id, hash_tokens(&text));
        }
        let naive_size = b.clone().build_naive().byte_size();
        let succinct_bytes = b.build().to_bytes();
        // The target is a real savings; at this scale we expect
        // the succinct blob to be strictly smaller than the naive
        // flat-array baseline.
        assert!(
            succinct_bytes.len() < naive_size,
            "succinct {} should be < naive baseline {}",
            succinct_bytes.len(),
            naive_size,
        );
    }

    #[test]
    fn postings_scale_saves_space_vs_naive() {
        // 1000 docs × 3 postings each over 500 terms; with
        // log2(1001)=10-bit doc_idx + 10-bit offsets, the jerky
        // idx_bytes should be clearly smaller than a u32 doc_idx
        // + u32 offset stored naively (10 vs 32 bits).
        let mut lists = Vec::new();
        for t in 0..500 {
            let mut l = Vec::new();
            for j in 0..3 {
                l.push(((t * 3 + j) as u32 % 1000, 1.0 + j as f32));
            }
            lists.push(l);
        }
        let total: usize = lists.iter().map(|l| l.len()).sum();
        let (bytes, _meta) = SuccinctPostings::build(&lists, 1000).unwrap();
        // Naive layout would be:
        //   doc_idx  = 4 B × total_postings
        //   offsets  = 4 B × (n_terms + 1)
        //   scores   = 4 B × total_postings (f32)
        // = total × 8 + (n_terms + 1) × 4
        // The succinct single-region body packs all three into
        // bit-packed CompactVectors (doc_idx 10 bits, offsets
        // 11 bits, scores 16 bits at this scale) plus tiny
        // per-CompactVector overhead — strictly smaller.
        let naive = total * 4 + (lists.len() + 1) * 4 + total * 4;
        assert!(
            bytes.len() < naive,
            "succinct body {} < naive total {}",
            bytes.len(),
            naive
        );
    }
}
