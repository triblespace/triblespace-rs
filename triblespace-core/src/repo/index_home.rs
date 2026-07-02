//! Index-home: a log-structured merge tree (LSMT) of immutable,
//! content-addressed index *segments*, named by a mutable *manifest*
//! pinned at the branch head.
//!
//! # The problem
//!
//! Derived indexes (a [`SuccinctArchive`] rollup, a BM25 term index, an
//! HNSW vector graph) go stale when the source branch changes, and a
//! *monolithic* index over the whole branch is too expensive to rebuild
//! on every commit. The current [`Repository::compute_rollup`] pays the
//! acute form: a full `ws.checkout(..)` materialises the entire branch
//! into a [`TribleSet`] before a fast query. That checkout is the cost
//! the whole architecture exists to avoid.
//!
//! # The structure
//!
//! Each index *kind* is an LSMT of segments:
//!
//! - **Segments** — immutable, content-addressed blobs (one per
//!   maintenance step, plus merged segments). Local cache blobs, GC'd
//!   when unreferenced.
//! - **Manifest** — a single mutable pointer at the branch head naming
//!   the current live segment set (with per-segment LSMT level + an
//!   optional per-kind stats slot). Overwritten (not history-appended)
//!   as the index evolves; the old manifest and its now-orphaned
//!   segments become GC-able.
//! - **Read** — one head lookup to the manifest, then union-query the
//!   referenced segments (bounded fan-out). No commit walk, no checkout.
//! - **Maintain** — [`IndexHome::update_index`] appends a small new
//!   segment (cheap) and runs a size-tiered merge to bound fan-out; the
//!   head manifest is overwritten and the superseded segments become
//!   orphans for GC.
//!
//! # Attach point: a dedicated pin, not `commit_metadata`
//!
//! The manifest is stored as a **dedicated [`PinStore`] pin** — its own
//! mutable cell, whose id is derived deterministically from
//! `(source_branch, kind)` via [`manifest_pin_id`]. This is the right
//! attach point for three reasons the design fragment
//! (wiki:100CE93A263F9308F4460A894BE323FE) calls load-bearing:
//!
//! 1. **Head-not-commit.** A pin is the one mutable, *not*
//!    version-controlled pointer in the substrate. Attaching the
//!    manifest to `commit_metadata` (a per-commit fact) would make it
//!    accumulate in history and force "the current index" to be the
//!    union of every commit's delta — the walk-all-commits problem,
//!    worse than a checkout. The pin holds the *complete* current set as
//!    one overwrite.
//! 2. **GC-able.** The pile compaction sweep treats every pin head as a
//!    reachability root (see [`reachable`](crate::repo::reachable) /
//!    [`Yard`](crate::repo::yard::Yard)). Segments referenced by the
//!    live manifest survive; superseded segments and old manifest blobs
//!    become unreachable and are reclaimed by the *existing* store GC —
//!    no bespoke collector.
//! 3. **Ephemeral / local.** A manifest pin carries no
//!    `metadata::name`, so it is not a content branch; it is re-derivable
//!    from the commit chain and can be excluded from sync (a `local_only`
//!    tagging is the intended seam, see below). The existing
//!    single-blob `rollup` branch-metadata attribute is the monolithic
//!    predecessor of this design; the manifest generalises it to an LSMT
//!    without touching the branch-metadata format.
//!
//! # Reuse of the Yard LSM machinery
//!
//! [`Yard`](crate::repo::yard::Yard) already implements this LSM shape
//! for *blobs* (young-to-old generations, union reads, size-triggered
//! tenuring, reachability-keep + reclaim). The index-home LSMT is that
//! shape applied to *indexes*: the manifest is the generation list, the
//! segments are the generations, and GC is Yard's reclaim. We reuse the
//! store's reachability GC directly and mirror the size-tiered tenuring
//! policy in [`IndexHome::update_index`].
//!
//! # Seams left for follow-up work
//!
//! - **GPU merge.** [`IndexKind::merge`] is CPU today (union-then-rebuild
//!   for the SuccinctArchive rollup). The GPU-accelerated succinct merge
//!   (compass:09ce3667) drops in behind this one method — the surface,
//!   manifest, and tiering are unaffected.
//! - **Commit hook.** v1 exposes an *explicit* [`IndexHome::update_index`]
//!   entry point. Wiring it automatically into the commit path (selective
//!   via `pattern_changes!` over each kind's source attribute) touches
//!   the commit path and is a **reviewed follow-up**, deliberately not
//!   done here.
//! - **Per-segment stats.** The manifest schema carries an optional
//!   opaque per-segment stats blob ([`IndexKind::stats`], default
//!   `None`). The SuccinctArchive rollup leaves it empty; BM25
//!   (`doc_frequency`/`n_docs`) and HNSW (per-segment count) will
//!   populate it so a query can read a scalar without opening every
//!   segment blob.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use crate::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob, Universe};
use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::id::{Id, RawId};
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::encodings::iu256::U256BE;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::query::unionconstraint::UnionConstraint;
use crate::query::{TriblePattern, Variable};
use crate::repo::{BlobStore, BlobStoreGet, PinStore, PushResult};
use crate::trible::TribleSet;

use crate::find;
use crate::prelude::{attributes, entity, pattern};

attributes! {
    /// Handle of an immutable index segment blob (schema-agnostic — the
    /// owning [`IndexKind`] knows how to `attach` it).
    "CEF658631FD636FB59C139E8C8EEECCE" as pub seg_blob: Handle<UnknownBlob>;
    /// LSMT level (tier) of a segment. Level 0 holds freshly appended
    /// segments; a size-tiered merge tenures a full tier into a single
    /// segment one level up.
    "7188AAD5C5044798547E7F53FE1CA5D5" as pub seg_level: U256BE;
    /// Monotonic sequence number, assigned per segment as it enters the
    /// manifest. Gives a stable total order within a level and keeps
    /// content-addressed segment entities distinct.
    "DFE499897718CFB97497AA8504A5D48F" as pub seg_seq: U256BE;
    /// Optional per-kind lightweight stats blob for a segment. Absent for
    /// kinds (like the SuccinctArchive rollup) that don't use it.
    "5387B92C012F03C705169A789347528F" as pub seg_stats: Handle<UnknownBlob>;
}

/// Number of segments a level may hold before a size-tiered merge
/// tenures the whole tier into a single segment one level up. Mirrors
/// Yard's tenuring trigger; keeps read fan-out bounded by
/// `FANOUT * log_FANOUT(N)`.
pub const FANOUT: usize = 4;

/// What a derived index *is*: how to build a segment from source tribles,
/// how to attach a stored segment into a queryable form, and how to merge
/// segments (CPU).
///
/// The [`IndexHome`] surface owns *when and where* (manifest, latest-wins
/// overwrite, size-tiered merge, GC); a kind owns *what* (the segment
/// format and its query/merge semantics).
pub trait IndexKind {
    /// In-memory, queryable attachment of a single stored segment.
    type Segment;

    /// Stable id identifying this kind. Combined with the source branch
    /// id to derive the manifest pin (see [`manifest_pin_id`]), so two
    /// kinds over the same branch get independent manifests.
    fn kind_id(&self) -> Id;

    /// Build a segment blob from a source trible view (typically a
    /// bounded commit-range delta, never the whole branch).
    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob>;

    /// Attach a stored segment blob into its queryable form.
    ///
    /// The blob must be one previously produced by [`build`](Self::build)
    /// or [`merge`](Self::merge) of the same kind; segments are
    /// content-addressed and produced by this surface, so a decode
    /// failure is a corruption bug, not an expected condition.
    fn attach(&self, blob: Blob<UnknownBlob>) -> Self::Segment;

    /// Merge several attached segments into one new segment blob (CPU).
    ///
    /// This is the LSMT maintenance primitive. The GPU-accelerated
    /// succinct merge drops in behind exactly this method.
    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob>;

    /// Optional lightweight per-segment stats, serialised into the
    /// manifest so a query can read a scalar without opening the segment
    /// blob. Default `None`; consumers that need it (BM25, HNSW) override.
    fn stats(&self, _segment: &Self::Segment) -> Option<Blob<UnknownBlob>> {
        None
    }
}

/// One entry in a [`Manifest`]: a live segment, its LSMT level, its
/// sequence number, and an optional stats-blob handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentEntry {
    /// Handle of the immutable segment blob.
    pub blob: Inline<Handle<UnknownBlob>>,
    /// LSMT level of the segment.
    pub level: u64,
    /// Sequence number (total order within a level).
    pub seq: u64,
    /// Optional per-kind stats blob handle.
    pub stats: Option<Inline<Handle<UnknownBlob>>>,
}

/// The set of live segments for one index, ordered by `(level, seq)`.
///
/// Serialises to/from a [`TribleSet`] (one entity per segment) that is
/// stored as the manifest pin's head blob. This is the mutable head
/// state: overwritten as a whole on each [`IndexHome::update_index`].
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    /// Live segments, kept sorted by `(level, seq)`.
    pub segments: Vec<SegmentEntry>,
    next_seq: u64,
}

impl Manifest {
    /// An empty manifest (no segments yet).
    pub fn empty() -> Self {
        Self { segments: Vec::new(), next_seq: 0 }
    }

    /// Number of live segments.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Whether the manifest names no segments.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Append a segment, assigning it the next sequence number, then keep
    /// the segment list ordered by `(level, seq)`.
    fn push(&mut self, blob: Inline<Handle<UnknownBlob>>, level: u64, stats: Option<Inline<Handle<UnknownBlob>>>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.segments.push(SegmentEntry { blob, level, seq, stats });
        self.segments.sort_by_key(|e| (e.level, e.seq));
    }

    /// The lowest LSMT level holding at least `fanout` segments, if any.
    fn overflowing_level(&self, fanout: usize) -> Option<u64> {
        let mut counts: HashMap<u64, usize> = HashMap::new();
        for e in &self.segments {
            *counts.entry(e.level).or_default() += 1;
        }
        counts
            .into_iter()
            .filter(|&(_, n)| n >= fanout)
            .map(|(level, _)| level)
            .min()
    }

    /// Remove and return every segment at `level`.
    fn drain_level(&mut self, level: u64) -> Vec<SegmentEntry> {
        let (victims, keep): (Vec<_>, Vec<_>) =
            self.segments.iter().partition(|e| e.level == level);
        self.segments = keep;
        victims
    }

    /// Parse a manifest from its serialised [`TribleSet`] form.
    pub fn from_tribles(set: &TribleSet) -> Self {
        // Collect optional per-segment stats keyed by segment-entity id.
        let mut stats_map: HashMap<[u8; 32], Inline<Handle<UnknownBlob>>> = HashMap::new();
        for (e, st) in find!(
            (e: Inline<GenId>, st: Inline<Handle<UnknownBlob>>),
            pattern!(set, [{ ?e @ seg_stats: ?st }])
        ) {
            stats_map.insert(e.raw, st);
        }

        let mut segments: Vec<SegmentEntry> = Vec::new();
        let mut max_seq: Option<u64> = None;
        for (e, blob, level, seq) in find!(
            (
                e: Inline<GenId>,
                blob: Inline<Handle<UnknownBlob>>,
                level: Inline<U256BE>,
                seq: Inline<U256BE>
            ),
            pattern!(set, [{ ?e @ seg_blob: ?blob, seg_level: ?level, seg_seq: ?seq }])
        ) {
            let level = level.try_from_inline::<u64>().unwrap_or(0);
            let seq = seq.try_from_inline::<u64>().unwrap_or(0);
            max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            let stats = stats_map.get(&e.raw).copied();
            segments.push(SegmentEntry { blob, level, seq, stats });
        }
        segments.sort_by_key(|e| (e.level, e.seq));

        Self {
            next_seq: max_seq.map_or(0, |m| m + 1),
            segments,
        }
    }

    /// Serialise the manifest to a [`TribleSet`] (one entity per segment).
    pub fn to_tribles(&self) -> TribleSet {
        let mut set = TribleSet::new();
        for e in &self.segments {
            set += entity! {
                seg_blob: e.blob,
                seg_level: e.level,
                seg_seq: e.seq,
                seg_stats?: e.stats,
            };
        }
        set
    }
}

/// Error surfaced by the [`IndexHome`] surface.
#[derive(Debug)]
pub enum IndexError {
    /// An underlying storage operation failed.
    Storage(Box<dyn Error + Send + Sync>),
    /// The manifest pin advanced between read and CAS-write. The caller
    /// may retry — segment blobs are content-addressed, so a retry
    /// dedupes against already-uploaded blobs.
    Conflict,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Storage(e) => write!(f, "index-home storage error: {e}"),
            IndexError::Conflict => write!(f, "index-home manifest pin advanced concurrently"),
        }
    }
}

impl Error for IndexError {}

fn boxed<E: Error + Send + Sync + 'static>(e: E) -> IndexError {
    IndexError::Storage(Box::new(e))
}

/// Deterministic manifest pin id for a `(source_branch, kind)` pair.
///
/// Derived by hashing a domain-separation tag with the two ids, so the
/// manifest can always be re-found without extra bookkeeping and two
/// kinds over the same branch never collide.
pub fn manifest_pin_id(source_branch: Id, kind: Id) -> Id {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"triblespace.index_home.manifest.v1");
    hasher.update(&source_branch.raw());
    hasher.update(&kind.raw());
    let digest = hasher.finalize();
    let mut raw: RawId = [0u8; 16];
    raw.copy_from_slice(&digest.as_bytes()[..16]);
    // A blake3 digest is astronomically never all-zero, but keep the id
    // non-nil unconditionally so this can't ever return `None`.
    Id::new(raw).unwrap_or_else(|| {
        raw[0] |= 1;
        Id::new(raw).expect("non-nil after forcing low bit")
    })
}

/// The index-home surface for one `(source_branch, kind)`: reads and
/// maintains the LSMT of segments named by a manifest pin.
///
/// Generic over any storage that is both a [`BlobStore`] (segments +
/// manifest blobs) and a [`PinStore`] (the mutable manifest pin). GC of
/// orphaned segments is the store's own reachability sweep — see
/// [`Yard`](crate::repo::yard::Yard).
pub struct IndexHome<'s, S, K> {
    storage: &'s mut S,
    kind: K,
    pin: Id,
}

impl<'s, S, K> IndexHome<'s, S, K>
where
    S: BlobStore + PinStore,
    K: IndexKind,
{
    /// Open the index home for `kind` over `source_branch`, backed by
    /// `storage`. Does not touch storage until a read or update.
    pub fn new(storage: &'s mut S, source_branch: Id, kind: K) -> Self {
        let pin = manifest_pin_id(source_branch, kind.kind_id());
        Self { storage, kind, pin }
    }

    /// The manifest pin id (the mutable head cell for this index).
    pub fn pin(&self) -> Id {
        self.pin
    }

    fn head(&mut self) -> Result<Option<Inline<Handle<SimpleArchive>>>, IndexError> {
        self.storage.head(self.pin).map_err(boxed)
    }

    /// Read the current manifest (empty if the index has no segments yet).
    pub fn read_manifest(&mut self) -> Result<Manifest, IndexError> {
        match self.head()? {
            None => Ok(Manifest::empty()),
            Some(handle) => {
                let reader = self.storage.reader().map_err(boxed)?;
                let set: TribleSet = reader.get(handle).map_err(boxed)?;
                Ok(Manifest::from_tribles(&set))
            }
        }
    }

    fn put_segment(&mut self, blob: Blob<UnknownBlob>) -> Result<Inline<Handle<UnknownBlob>>, IndexError> {
        self.storage.put::<UnknownBlob, _>(blob).map_err(boxed)
    }

    fn attach_handle(&mut self, handle: Inline<Handle<UnknownBlob>>) -> Result<K::Segment, IndexError> {
        let reader = self.storage.reader().map_err(boxed)?;
        let blob: Blob<UnknownBlob> = reader.get(handle).map_err(boxed)?;
        Ok(self.kind.attach(blob))
    }

    /// Attach every live segment named by the manifest, ready for a
    /// union query. No checkout, no commit walk — one manifest lookup
    /// plus a bounded number of segment fetches.
    pub fn attach_all(&mut self) -> Result<Vec<K::Segment>, IndexError> {
        let manifest = self.read_manifest()?;
        let reader = self.storage.reader().map_err(boxed)?;
        let mut out = Vec::with_capacity(manifest.segments.len());
        for e in &manifest.segments {
            let blob: Blob<UnknownBlob> = reader.get(e.blob).map_err(boxed)?;
            out.push(self.kind.attach(blob));
        }
        Ok(out)
    }

    /// The explicit maintenance entry point (v1 trigger — no automatic
    /// commit hook).
    ///
    /// Builds a new level-0 segment from `source` (a bounded delta),
    /// runs a size-tiered merge to bound fan-out, and CAS-overwrites the
    /// manifest pin. Superseded segments and the old manifest become
    /// unreachable and are reclaimed by the store's GC.
    ///
    /// Returns [`IndexError::Conflict`] if the manifest pin advanced
    /// concurrently; the caller may retry.
    pub fn update_index(&mut self, source: &TribleSet) -> Result<(), IndexError> {
        let old_head = self.head()?;
        let mut manifest = match old_head {
            None => Manifest::empty(),
            Some(handle) => {
                let reader = self.storage.reader().map_err(boxed)?;
                let set: TribleSet = reader.get(handle).map_err(boxed)?;
                Manifest::from_tribles(&set)
            }
        };

        // Build + append a fresh level-0 segment. Attach the just-built
        // blob (zero-copy) to derive its optional stats without a store
        // round-trip.
        let segment_blob = self.kind.build(source);
        let attached = self.kind.attach(segment_blob.clone());
        let stats_blob = self.kind.stats(&attached);
        let handle = self.put_segment(segment_blob)?;
        let stats_handle = match stats_blob {
            Some(b) => Some(self.put_segment(b)?),
            None => None,
        };
        manifest.push(handle, 0, stats_handle);

        // Size-tiered merge: while a level overflows, tenure the whole
        // tier into a single merged segment one level up (Yard's shape).
        while let Some(level) = manifest.overflowing_level(FANOUT) {
            let victims = manifest.drain_level(level);
            let mut attached = Vec::with_capacity(victims.len());
            for v in &victims {
                attached.push(self.attach_handle(v.blob)?);
            }
            let merged_blob = self.kind.merge(&attached);
            let merged_seg = self.kind.attach(merged_blob.clone());
            let merged_stats = self.kind.stats(&merged_seg);
            let merged_handle = self.put_segment(merged_blob)?;
            let merged_stats_handle = match merged_stats {
                Some(b) => Some(self.put_segment(b)?),
                None => None,
            };
            manifest.push(merged_handle, level + 1, merged_stats_handle);
        }

        // Overwrite the manifest pin via CAS.
        let new_set = manifest.to_tribles();
        let new_head: Inline<Handle<SimpleArchive>> = self.storage.put(new_set).map_err(boxed)?;
        match self.storage.update(self.pin, old_head, Some(new_head)).map_err(boxed)? {
            PushResult::Success() => Ok(()),
            PushResult::Conflict(_) => Err(IndexError::Conflict),
        }
    }
}

// ---------------------------------------------------------------------------
// First consumer: the SuccinctArchive rollup.
// ---------------------------------------------------------------------------

/// The first [`IndexKind`]: a rollup whose segments are
/// [`SuccinctArchive`]s.
///
/// - `build` — construct a [`SuccinctArchive`] over the source tribles
///   (sort-based; the archive's `From<&TribleSet>` does the domain sort +
///   wavelet freeze).
/// - `attach` — decode the blob into a [`SuccinctArchive`], mmap-queried
///   in place (zero-copy over the shared `Bytes`).
/// - `merge` — **CPU** union-then-rebuild: reconstruct each segment's
///   tribles, union them, and rebuild one archive. This is the clean seam
///   the GPU-accelerated succinct merge (sorted-run merge + wavelet
///   reassembly) replaces without touching the surface.
#[derive(Debug, Clone, Copy, Default)]
pub struct SuccinctRollup;

impl SuccinctRollup {
    /// Stable kind id for the SuccinctArchive rollup.
    pub fn new() -> Self {
        SuccinctRollup
    }

    /// A queryable view over a set of attached segments that unions them
    /// into one logical dataset — the correct LSMT read (a single match
    /// may span segments).
    pub fn union<'a>(segments: &'a [SuccinctArchive<OrderedUniverse>]) -> UnionArchive<'a, OrderedUniverse> {
        UnionArchive::new(segments)
    }
}

impl IndexKind for SuccinctRollup {
    type Segment = SuccinctArchive<OrderedUniverse>;

    fn kind_id(&self) -> Id {
        Id::from_hex("9540D50DEDECA9CA948FD14474F86566").expect("valid kind id")
    }

    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
        let archive: SuccinctArchive<OrderedUniverse> = source.into();
        {
            let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
            blob.transmute()
        }
    }

    fn attach(&self, blob: Blob<UnknownBlob>) -> Self::Segment {
        blob.transmute::<SuccinctArchiveBlob>()
            .try_from_blob()
            .expect("valid succinct-archive segment blob")
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        // CPU union-then-rebuild. GPU-merge seam: replace this body with a
        // structural sorted-run merge + wavelet reassembly.
        let mut union = TribleSet::new();
        for seg in segments {
            let set: TribleSet = seg.into();
            union += set;
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&union).into();
        {
            let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
            blob.transmute()
        }
    }
}

/// A [`TriblePattern`] view that unions several [`SuccinctArchive`]
/// segments into one logical dataset.
///
/// Correct LSMT read semantics: the full dataset is the union of the
/// segments, and a single pattern clause matches a trible in *any*
/// segment. Each per-clause constraint is therefore a
/// [`UnionConstraint`] over the segments' per-clause constraints, and the
/// engine's conjunction across clauses joins freely across segment
/// boundaries — so a match whose tribles live in different segments is
/// found, which per-segment querying would miss.
pub struct UnionArchive<'a, U> {
    segments: &'a [SuccinctArchive<U>],
}

impl<'a, U> UnionArchive<'a, U> {
    /// Wrap a slice of attached segments.
    ///
    /// # Panics
    ///
    /// Querying an empty union panics inside [`UnionConstraint`] (a
    /// zero-arm union has no variable set). Callers should skip the query
    /// when there are no segments — an empty index yields no rows.
    pub fn new(segments: &'a [SuccinctArchive<U>]) -> Self {
        Self { segments }
    }
}

impl<'a, U> TriblePattern for UnionArchive<'a, U>
where
    U: Universe + Send + Sync,
{
    type PatternConstraint<'p>
        = UnionConstraint<<SuccinctArchive<U> as TriblePattern>::PatternConstraint<'p>>
    where
        Self: 'p;

    fn pattern<'p, V: InlineEncoding>(
        &'p self,
        e: Variable<GenId>,
        a: Variable<GenId>,
        v: Variable<V>,
    ) -> Self::PatternConstraint<'p> {
        let constraints = self
            .segments
            .iter()
            .map(|s| s.pattern(e, a, v))
            .collect::<Vec<_>>();
        UnionConstraint::new(constraints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::examples::literature;
    use crate::id::fucid;
    use crate::inline::TryToInline;
    use crate::repo::memoryrepo::MemoryRepo;

    fn person(name: &str) -> TribleSet {
        let id = fucid();
        entity! { &id @ literature::firstname: name }.into()
    }

    // A trivial source branch id (index-home never reads it as a branch;
    // it only seeds the manifest-pin derivation).
    fn source_branch() -> Id {
        *fucid()
    }

    #[test]
    fn manifest_roundtrips_through_tribles() {
        let mut m = Manifest::empty();
        let h0 = Inline::<Handle<UnknownBlob>>::new([1u8; 32]);
        let h1 = Inline::<Handle<UnknownBlob>>::new([2u8; 32]);
        let st = Inline::<Handle<UnknownBlob>>::new([3u8; 32]);
        m.push(h0, 0, None);
        m.push(h1, 1, Some(st));

        let parsed = Manifest::from_tribles(&m.to_tribles());
        assert_eq!(parsed.segments.len(), 2);
        // Ordered by (level, seq).
        assert_eq!(parsed.segments[0].blob, h0);
        assert_eq!(parsed.segments[0].level, 0);
        assert_eq!(parsed.segments[1].blob, h1);
        assert_eq!(parsed.segments[1].level, 1);
        assert_eq!(parsed.segments[1].stats, Some(st));
        // next_seq continues past the max seq read back.
        assert_eq!(parsed.next_seq, 2);
    }

    #[test]
    fn manifest_latest_wins() {
        let mut storage = MemoryRepo::default();
        let branch = source_branch();
        let da = person("Ada");
        let db = person("Grace");

        {
            let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
            home.update_index(&da).unwrap();
            assert_eq!(home.read_manifest().unwrap().len(), 1);
        }
        let first_head = storage.head(manifest_pin_id(branch, SuccinctRollup.kind_id())).unwrap();

        {
            let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
            home.update_index(&db).unwrap();
            let m = home.read_manifest().unwrap();
            // FANOUT is 4, so two updates leave two level-0 segments.
            assert_eq!(m.len(), 2);
        }
        let second_head = storage.head(manifest_pin_id(branch, SuccinctRollup.kind_id())).unwrap();

        // The pin now names the newest manifest, not the first.
        assert_ne!(first_head, second_head);
    }

    #[test]
    fn union_read_across_segments() {
        // A match that spans two segments: A's `author`->B lands in
        // segment 1, B's name lands in segment 2. Per-segment querying
        // would miss it; the union read must find it.
        let mut storage = MemoryRepo::default();
        let branch = source_branch();

        let a = fucid();
        let b = fucid();

        // Segment 1: A authored, pointing at B (B has no attributes here).
        let s1 = entity! { &a @ literature::title: "Middlemarch", literature::author: &b };
        // Segment 2: B's name.
        let s2 = entity! { &b @ literature::firstname: "George" };

        let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
        home.update_index(&s1).unwrap();
        home.update_index(&s2).unwrap();
        let m = home.read_manifest().unwrap();
        assert_eq!(m.len(), 2, "expected two independent segments");

        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);

        let rows: Vec<_> = find!(
            (name: Inline<_>),
            pattern!(&union, [
                { _?book @ literature::title: "Middlemarch", literature::author: _?author },
                { _?author @ literature::firstname: ?name }
            ])
        )
        .collect();

        assert_eq!(rows.len(), 1, "cross-segment join must resolve");
        assert_eq!(rows[0].0, "George".try_to_inline().unwrap());
    }

    #[test]
    fn merge_equals_rebuild_from_union() {
        // The kind-level merge of two segments must equal a single archive
        // built from the union of both sources.
        let kind = SuccinctRollup::new();
        let da = person("Ada");
        let db = person("Grace");

        let seg_a = kind.attach(kind.build(&da));
        let seg_b = kind.attach(kind.build(&db));
        let merged = kind.attach(kind.merge(&[seg_a, seg_b]));
        let merged_set: TribleSet = (&merged).into();

        let mut union = da;
        union += db;
        assert_eq!(merged_set, union);
    }

    #[test]
    fn compaction_bounds_fanout_and_preserves_data() {
        // Enough updates to trigger the size-tiered merge; the union read
        // must still return every fact, and no level may hold >= FANOUT.
        let mut storage = MemoryRepo::default();
        let branch = source_branch();

        let names = ["a", "b", "c", "d", "e", "f", "g"];
        {
            let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
            for n in names {
                home.update_index(&person(n)).unwrap();
            }
            let m = home.read_manifest().unwrap();
            let mut per_level: HashMap<u64, usize> = HashMap::new();
            for e in &m.segments {
                *per_level.entry(e.level).or_default() += 1;
            }
            assert!(per_level.values().all(|&n| n < FANOUT), "fan-out bounded");
            // A merge must have happened (fewer segments than updates).
            assert!(m.len() < names.len());
        }

        let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        let count = find!(
            (name: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?name }])
        )
        .count();
        assert_eq!(count, names.len(), "all facts survive compaction");
    }

    #[test]
    fn query_without_checkout_matches_checkout() {
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch = repo.create_branch("main", None).unwrap();

        // Commit across three rounds; feed each round's delta to the
        // index as a separate segment.
        let mut deltas = Vec::new();
        for round in 0..3 {
            let mut ws = repo.pull(*branch).unwrap();
            let mut delta = TribleSet::new();
            for i in 0..4 {
                delta += person(&format!("p{round}_{i}"));
            }
            ws.commit(delta.clone(), "round");
            repo.push(&mut ws).unwrap();
            deltas.push(delta);
        }

        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            for d in &deltas {
                home.update_index(d).unwrap();
            }
        }

        // Reference: the full checkout query.
        let checkout = repo.pull(*branch).unwrap().checkout(..).unwrap();
        let mut expected: Vec<_> = find!(
            (name: Inline<_>),
            pattern!(&*checkout, [{ _?p @ literature::firstname: ?name }])
        )
        .collect();
        expected.sort();

        // Under test: the same query against the manifest + segments,
        // with no checkout of the branch.
        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        let mut got: Vec<_> = find!(
            (name: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?name }])
        )
        .collect();
        got.sort();

        assert_eq!(got, expected);
        assert_eq!(got.len(), 12);
    }

    #[test]
    fn gc_reclaims_orphaned_segments() {
        // Drive the index over a Yard so the store's reachability GC runs.
        // After enough updates to trigger a merge, the merged-away level-0
        // segments are unreachable from the manifest pin and must be
        // reclaimed, while the live merged segment survives and the union
        // read still returns every fact.
        use crate::repo::yard::{Yard, YardConfig};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gen-0.pile");
        let mut yard = Yard::create(vec![path], YardConfig::default()).unwrap();

        let branch = source_branch();

        // FANOUT updates fill level 0 and tenure it into one level-1
        // segment; the FANOUT level-0 segments become orphans.
        let mut deltas = Vec::new();
        for i in 0..FANOUT {
            deltas.push(person(&format!("n{i}")));
        }
        // Content-addressed handles of the level-0 segments we expect to be
        // orphaned after the merge.
        let orphan_handles: Vec<Inline<Handle<UnknownBlob>>> = deltas
            .iter()
            .map(|d| SuccinctRollup.build(d).get_handle())
            .collect();

        {
            let mut home = IndexHome::new(&mut yard, branch, SuccinctRollup::new());
            for d in &deltas {
                home.update_index(d).unwrap();
            }
            let m = home.read_manifest().unwrap();
            // A single tenured segment at level 1 (the merge fired).
            assert_eq!(m.len(), 1);
            assert_eq!(m.segments[0].level, 1);
        }

        // Before GC the orphan blobs are still physically resident.
        {
            let reader = yard.reader().unwrap();
            for h in &orphan_handles {
                assert!(
                    reader.get::<Blob<UnknownBlob>, UnknownBlob>(*h).is_ok(),
                    "orphan segment resident before GC"
                );
            }
        }

        // Reachability GC + physical reclaim.
        yard.collect().unwrap();
        yard.reclaim().unwrap();

        // The orphaned level-0 segments are gone.
        {
            let reader = yard.reader().unwrap();
            for h in &orphan_handles {
                assert!(
                    reader.get::<Blob<UnknownBlob>, UnknownBlob>(*h).is_err(),
                    "orphan segment reclaimed after GC"
                );
            }
        }

        // The live merged segment survived: the union read still resolves
        // every fact.
        let mut home = IndexHome::new(&mut yard, branch, SuccinctRollup::new());
        let segments = home.attach_all().unwrap();
        assert_eq!(segments.len(), 1);
        let union = SuccinctRollup::union(&segments);
        let count = find!(
            (name: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?name }])
        )
        .count();
        assert_eq!(count, FANOUT, "all facts survive GC");
    }

    // Timing demo: query-without-checkout vs a full checkout, on a
    // synthetic branch. Ignored by default (it builds a sizable dataset);
    // run with:
    //   cargo test -p triblespace-core --lib \
    //     repo::index_home::tests::bench_query_without_checkout \
    //     -- --ignored --nocapture
    #[test]
    #[ignore]
    fn bench_query_without_checkout() {
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        use std::time::Instant;

        const ROUNDS: usize = 40;
        const PER_ROUND: usize = 2_000;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch = repo.create_branch("main", None).unwrap();

        // Commit ROUNDS batches; index each batch as its own segment.
        // Track one known name so the point lookup has a definite target.
        let mut probe_name = String::new();
        for round in 0..ROUNDS {
            let mut ws = repo.pull(*branch).unwrap();
            let mut delta = TribleSet::new();
            for i in 0..PER_ROUND {
                let name = format!("person_{round}_{i}");
                if round == ROUNDS / 2 && i == PER_ROUND / 2 {
                    probe_name = name.clone();
                }
                delta += person(&name);
            }
            ws.commit(delta.clone(), "round");
            repo.push(&mut ws).unwrap();
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            home.update_index(&delta).unwrap();
        }

        let total = ROUNDS * PER_ROUND;

        // (1) Full checkout + point lookup.
        let t0 = Instant::now();
        let checkout = repo.pull(*branch).unwrap().checkout(..).unwrap();
        let checkout_time = t0.elapsed();
        let t1 = Instant::now();
        let via_checkout = find!(
            (p: Inline<_>),
            pattern!(&*checkout, [{ ?p @ literature::firstname: probe_name.as_str() }])
        )
        .count();
        let checkout_query_time = t1.elapsed();

        // (2) Manifest attach + union point lookup — no checkout.
        let t2 = Instant::now();
        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let segments = home.attach_all().unwrap();
        let attach_time = t2.elapsed();
        let t3 = Instant::now();
        let union = SuccinctRollup::union(&segments);
        let via_index = find!(
            (p: Inline<_>),
            pattern!(&union, [{ ?p @ literature::firstname: probe_name.as_str() }])
        )
        .count();
        let index_query_time = t3.elapsed();

        assert_eq!(via_checkout, via_index);
        assert_eq!(via_index, 1);

        eprintln!("index-home timing demo: {total} tribles across {ROUNDS} segments");
        eprintln!(
            "  checkout:  materialise {checkout_time:?} + query {checkout_query_time:?} = {:?}",
            checkout_time + checkout_query_time
        );
        eprintln!(
            "  index:     attach {attach_time:?} + query {index_query_time:?} = {:?}",
            attach_time + index_query_time
        );
    }
}
