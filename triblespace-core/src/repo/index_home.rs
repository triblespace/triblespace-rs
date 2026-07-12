//! Index-home: a log-structured merge tree (LSMT) of immutable,
//! content-addressed index *segments*, named by a *manifest* that lives
//! as tribles inside the branch-head tribleset.
//!
//! # The problem
//!
//! Derived indexes (a [`SuccinctArchive`](crate::blob::encodings::succinctarchive::SuccinctArchive) rollup, a BM25 term index, an
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
//! - **Segments** — immutable, content-addressed blobs (normally one
//!   logical level-zero leaf per source commit, plus merged segments; one
//!   unusually large commit may use several physical shards). Local cache
//!   blobs, GC'd when unreferenced.
//! - **Manifest** — a set of tribles (one entity per segment, tagged
//!   with the owning kind, carrying the segment blob handle, its LSMT
//!   level, and a sequence number, plus the source-commit coverage frontier)
//!   unioned
//!   directly into the branch-head tribleset. Rewritten (this kind's old
//!   segment tribles differenced out, the new ones unioned in) as the
//!   index evolves; the old branch-head-tribleset version and its
//!   now-orphaned segments become GC-able.
//! - **Read** — one branch-head lookup to the tribleset, select this
//!   kind's manifest subset via `pattern!`, then union-query the
//!   referenced segments (bounded fan-out). No commit walk, no checkout.
//! - **Maintain** — [`IndexHome::update_index`](crate::repo::index_home::IndexHome::update_index) appends a small new
//!   segment (cheap) and runs a size-tiered merge to bound fan-out; a new
//!   branch-head-tribleset version carries the rewritten manifest and the
//!   superseded segments become orphans for GC.
//!
//! # Attach point: unioned into the branch-head tribleset
//!
//! The manifest is **not** a separate pin. Its tribles are unioned
//! directly into the **branch-head tribleset** — the blob the branch pin
//! already references (the branch-metadata [`SimpleArchive`] built by
//! [`branch_metadata`](crate::repo::branch::branch_metadata)). The nice
//! thing about triblesets is that you can just union more data into them:
//! existing queries that care for their own attribute subset (the branch
//! `name`/`head`/`signature`/`rollup` facts, or another index kind's
//! segments) don't change. The index-manifest facts and the
//! branch-metadata facts coexist in one tribleset; every existing query
//! reads its own subset and is unaffected by the added index tribles.
//! This is the right attach point for three reasons the design fragment
//! (wiki:100CE93A263F9308F4460A894BE323FE) calls load-bearing:
//!
//! 1. **Head-not-commit.** The branch pin is the one mutable, *not*
//!    version-controlled pointer in the substrate. Attaching the manifest
//!    to `commit_metadata` (a per-commit fact) would make it accumulate
//!    in history and force "the current index" to be the union of every
//!    commit's delta — the walk-all-commits problem, worse than a
//!    checkout. Unioned into the branch head, the manifest holds the
//!    *complete* current set as one repoint of the branch pin.
//! 2. **GC-able.** The pile compaction sweep treats every pin head as a
//!    reachability root (see [`reachable`](crate::repo::reachable) /
//!    [`Yard`](crate::repo::yard::Yard)). Segment handles are values in
//!    the branch-head tribleset, so segments referenced by the live
//!    manifest survive as a side effect of the branch head being a
//!    reachability root; superseded segments and the old branch-head
//!    tribleset become unreachable and are reclaimed by the *existing*
//!    store GC — no bespoke collector and no separate manifest-pin path.
//! 3. **Ephemeral / soft-state.** The manifest is redundant with (and
//!    re-derivable from) the commit chain. Branch-metadata rebuilds carry it
//!    forward, while its source-commit frontier makes a missed maintenance
//!    hook detectable and restartable. Each
//!    segment entity is tagged with its kind id ([`seg_kind`](crate::repo::index_home::seg_kind)), so two
//!    kinds over the same branch keep independent manifests in the one
//!    tribleset. The existing single-blob `rollup` branch-metadata
//!    attribute is the monolithic predecessor of this design; the
//!    manifest generalises it to an LSMT that lives in the same
//!    branch-head tribleset.
//!
//! # Reuse of the Yard LSM machinery
//!
//! [`Yard`](crate::repo::yard::Yard) already implements this LSM shape
//! for *blobs* (young-to-old generations, union reads, size-triggered
//! tenuring, reachability-keep + reclaim). The index-home LSMT is that
//! shape applied to *indexes*: the manifest is the generation list, the
//! segments are the generations, and GC is Yard's reclaim. We reuse the
//! store's reachability GC directly and mirror the size-tiered tenuring
//! policy in [`IndexHome::update_index`](crate::repo::index_home::IndexHome::update_index).
//!
//! # Maintenance triggers
//!
//! Two entry points share one implementation ([`append_segment`](crate::repo::index_home::append_segment)):
//!
//! - **Explicit** — [`IndexHome::update_index`](crate::repo::index_home::IndexHome::update_index): build a segment from a
//!   caller-supplied delta and CAS the branch pin yourself.
//! - **On commit** — [`Repository::register_index`]
//!   (or the general [`Repository::on_commit`]): a hook runs inside the
//!   push and folds the new segment's manifest into the *same*
//!   branch-head tribleset the push is about to CAS in, so every commit
//!   maintains the index incrementally from its own delta with no second
//!   CAS and no race against the branch pin.
//!
//! [`Repository::register_index`]: crate::repo::Repository::register_index
//! [`Repository::on_commit`]: crate::repo::Repository::on_commit
//!
//! # Example
//!
//! `examples/index_home.rs` (`cargo run --example index_home` from the
//! workspace root) shows the whole loop against a temp pile:
//! `register_index` once, plain commits after, then a
//! query-without-checkout via [`IndexHome::attach_all`](crate::repo::index_home::IndexHome::attach_all)
//! and a union query over the attached segments.
//!
//! # Acceleration seam
//!
//! [`SuccinctRollup`](crate::repo::index_home::SuccinctRollup) uses the
//! bounded-memory CPU structural merge.
//! [`AcceleratedSuccinctRollup`](crate::repo::index_home::AcceleratedSuccinctRollup)
//! delegates only sufficiently large wavelet freezes to an external backend,
//! retaining the same manifest, kind id, and canonical segment bytes. A
//! returned backend error opens a circuit breaker and falls back to the CPU
//! implementation.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::succinctarchive::{
    merge_ordered_archives, merge_ordered_archives_with_backend, OrderedUniverse, SuccinctArchive,
    SuccinctArchiveBlob, Universe, WaveletMatrixFreezeBackend,
};
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::id::Id;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::encodings::iu256::U256BE;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::query::unionconstraint::UnionConstraint;
use crate::query::{Term, TriblePattern};
use crate::repo::{BlobStore, BlobStoreGet, PinStore, PushResult};
use crate::trible::TribleSet;

use crate::find;
use crate::prelude::{attributes, entity, pattern};

attributes! {
    /// Kind id owning this segment. Since every kind's manifest lives in
    /// the *same* branch-head tribleset, each segment entity is tagged
    /// with its owning [`IndexKind::kind_id`] so a read selects exactly
    /// one kind's segments and two kinds over the same branch keep
    /// independent manifests.
    "383FDDECB0317E1DC1CC6D11B38CE174" as pub seg_kind: GenId;
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
    /// Maximal source commits covered by this kind's live manifest. The
    /// repeated values form an antichain frontier: the indexed source set is
    /// the union of the tips' ancestor closures. A fully caught-up index has
    /// exactly the source branch HEAD as its sole tip.
    "C1D931CEA5CB365254E1C9FB349E9BA1" as pub covered_tip: Handle<SimpleArchive>;
}

/// Number of segments a level may hold before a size-tiered merge
/// tenures the whole tier into a single segment one level up. Mirrors
/// Yard's tenuring trigger; keeps read fan-out bounded by
/// `FANOUT * log_FANOUT(N)`.
pub const FANOUT: usize = 4;

/// What a derived index *is*: how to build a segment from source tribles,
/// how to attach a stored segment into a queryable form, and how to merge
/// segments.
///
/// The [`IndexHome`] surface owns *when and where* (manifest, latest-wins
/// overwrite, size-tiered merge, GC); a kind owns *what* (the segment
/// format and its query/merge semantics).
pub trait IndexKind {
    /// In-memory, queryable attachment of a single stored segment.
    type Segment;

    /// Stable id identifying this kind. Tagged onto every segment entity
    /// via [`seg_kind`], so two kinds sharing one branch-head tribleset
    /// keep independent manifests and each read selects exactly its own.
    fn kind_id(&self) -> Id;

    /// Build a segment blob from a source trible view (typically a
    /// bounded commit-range delta, never the whole branch).
    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob>;

    /// Fallibly attach a stored segment blob into its queryable form.
    ///
    /// The blob must be one previously produced by [`build`](Self::build)
    /// or [`merge`](Self::merge) of the same kind. A decode failure means a
    /// stale/corrupt manifest and is surfaced so repair can discard and
    /// rebuild it rather than panic or certify unreadable state.
    fn try_attach(
        &self,
        blob: Blob<UnknownBlob>,
    ) -> Result<Self::Segment, Box<dyn Error + Send + Sync>>;

    /// Attach a segment known to be valid, panicking on corruption.
    ///
    /// Maintenance and external reads should normally use the fallible
    /// index-home APIs, which call [`try_attach`](Self::try_attach). This
    /// convenience method remains useful in tests and trusted in-memory paths.
    fn attach(&self, blob: Blob<UnknownBlob>) -> Self::Segment {
        self.try_attach(blob)
            .unwrap_or_else(|err| panic!("invalid index segment: {err}"))
    }

    /// Merge several attached segments into one new segment blob.
    ///
    /// This is the LSMT maintenance primitive; each kind chooses its own
    /// execution backend while preserving its canonical segment format.
    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob>;
}

/// One entry in a [`Manifest`]: a live segment, its LSMT level, and its
/// sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentEntry {
    /// Handle of the immutable segment blob.
    pub blob: Inline<Handle<UnknownBlob>>,
    /// LSMT level of the segment.
    pub level: u64,
    /// Sequence number (total order within a level).
    pub seq: u64,
}

/// The set of live segments for one index kind, ordered by `(level, seq)`.
///
/// Serialises to/from the manifest subset of a [`TribleSet`] (one entity
/// per segment, each tagged with the owning [`seg_kind`]). Those tribles
/// are unioned into the branch-head tribleset alongside the branch
/// metadata and any other kind's segments; a read selects just this
/// kind's subset. This is the mutable head state: this kind's tribles are
/// rewritten as a whole on each [`IndexHome::update_index`].
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    /// Live segments, kept sorted by `(level, seq)`.
    pub segments: Vec<SegmentEntry>,
    /// Maximal source commits whose ancestor closures this manifest covers.
    /// Kept sorted and deduplicated for canonical serialization.
    pub covered: Vec<Inline<Handle<SimpleArchive>>>,
    next_seq: u64,
}

impl Manifest {
    /// Append a segment, assigning it the next sequence number, then keep
    /// the segment list ordered by `(level, seq)`.
    fn push(&mut self, blob: Inline<Handle<UnknownBlob>>, level: u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.segments.push(SegmentEntry { blob, level, seq });
        self.segments.sort_by_key(|e| (e.level, e.seq));
    }

    /// Adopt an already-built immutable segment at `level`.
    ///
    /// This is primarily a migration seam: a verified legacy monolithic
    /// rollup can become the initial upper-tier segment without decoding and
    /// rebuilding it. New source deltas should normally enter through
    /// [`append_segment`] at level zero.
    pub fn adopt_segment(&mut self, blob: Inline<Handle<UnknownBlob>>, level: u64) {
        self.push(blob, level);
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

    /// Parse the manifest for `kind` out of a branch-head [`TribleSet`].
    ///
    /// Selects exactly the segment entities tagged with `kind` via
    /// [`seg_kind`]; every other trible in the set (branch metadata, other
    /// kinds' segments) is ignored.
    pub fn from_tribles(set: &TribleSet, kind: Id) -> Self {
        let mut segments: Vec<SegmentEntry> = Vec::new();
        let mut max_seq: Option<u64> = None;
        for (_e, blob, level, seq) in find!(
            (
                e: Inline<GenId>,
                blob: Inline<Handle<UnknownBlob>>,
                level: Inline<U256BE>,
                seq: Inline<U256BE>
            ),
            pattern!(set, [{ ?e @ seg_kind: kind, seg_blob: ?blob, seg_level: ?level, seg_seq: ?seq }])
        ) {
            let level = level.try_from_inline::<u64>().unwrap_or(0);
            let seq = seq.try_from_inline::<u64>().unwrap_or(0);
            max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            segments.push(SegmentEntry { blob, level, seq });
        }
        segments.sort_by_key(|e| (e.level, e.seq));

        let mut covered: Vec<Inline<Handle<SimpleArchive>>> = find!(
            (tip: Inline<Handle<SimpleArchive>>),
            pattern!(set, [{ _?coverage @ seg_kind: kind, covered_tip: ?tip }])
        )
        .map(|(tip,)| tip)
        .collect();
        covered.sort_unstable_by_key(|tip| tip.raw);
        covered.dedup_by_key(|tip| tip.raw);

        Self {
            next_seq: max_seq.map_or(0, |m| m + 1),
            segments,
            covered,
        }
    }

    /// Serialise this kind's manifest to a [`TribleSet`] (one entity per
    /// segment, each tagged with `kind` via [`seg_kind`]). These tribles
    /// are unioned into the branch-head tribleset.
    pub fn to_tribles(&self, kind: Id) -> TribleSet {
        let mut set = TribleSet::new();
        for e in &self.segments {
            set += entity! {
                seg_kind: kind,
                seg_blob: e.blob,
                seg_level: e.level,
                seg_seq: e.seq,
            };
        }
        for tip in &self.covered {
            set += entity! {
                seg_kind: kind,
                covered_tip: *tip,
            };
        }
        set
    }

    /// True when this manifest certifies exactly the supplied source head.
    /// Empty source history is represented by an empty frontier; every
    /// non-empty, fully caught-up branch is represented by one HEAD tip.
    pub fn covers_head(&self, head: Option<Inline<Handle<SimpleArchive>>>) -> bool {
        match head {
            None => self.covered.is_empty(),
            Some(head) => self.covered.as_slice() == [head],
        }
    }

    /// Replace the coverage frontier with one fully covered source HEAD.
    pub fn cover_head(&mut self, head: Option<Inline<Handle<SimpleArchive>>>) {
        self.covered.clear();
        if let Some(head) = head {
            self.covered.push(head);
        }
    }

    /// Replace the coverage frontier with canonical sorted, unique tips.
    pub fn set_covered(&mut self, mut tips: Vec<Inline<Handle<SimpleArchive>>>) {
        tips.sort_unstable_by_key(|tip| tip.raw);
        tips.dedup_by_key(|tip| tip.raw);
        self.covered = tips;
    }
}

/// Replace one kind's complete manifest subset inside a branch-head set.
/// Every non-index trible and every other index kind is preserved verbatim.
pub fn replace_manifest(head_set: &mut TribleSet, kind: Id, manifest: &Manifest) {
    let old = Manifest::from_tribles(head_set, kind).to_tribles(kind);
    let mut next = head_set.difference(&old);
    next += manifest.to_tribles(kind);
    *head_set = next;
}

/// Remove one kind's segments and coverage certificate from a branch head.
pub fn clear_manifest(head_set: &mut TribleSet, kind: Id) {
    replace_manifest(head_set, kind, &Manifest::default());
}

/// Rewrite only one kind's coverage frontier, retaining its live segments.
pub fn set_coverage(head_set: &mut TribleSet, kind: Id, tips: Vec<Inline<Handle<SimpleArchive>>>) {
    let mut manifest = Manifest::from_tribles(head_set, kind);
    manifest.set_covered(tips);
    replace_manifest(head_set, kind, &manifest);
}

/// A live maintenance hook found that its manifest does not cover the source
/// HEAD on which the push is based. Advancing from that state would falsely
/// certify an unindexed gap, so the hook must leave the manifest untouched and
/// let an explicit bootstrap/repair fill the missing commits.
#[derive(Debug, Clone)]
pub struct CoverageMismatch {
    /// Index recipe whose certificate is stale.
    pub kind: Id,
    /// Source HEAD the incoming push assumes is already covered.
    pub expected: Option<Inline<Handle<SimpleArchive>>>,
    /// Frontier actually recorded in the manifest.
    pub actual: Vec<Inline<Handle<SimpleArchive>>>,
}

impl fmt::Display for CoverageMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "index {:x} coverage is stale: expected {:?}, found {:?}",
            self.kind, self.expected, self.actual
        )
    }
}

impl Error for CoverageMismatch {}

/// Extract the index-home manifest subset of a branch-head tribleset: every
/// segment entity across *all* kinds.
///
/// A branch-metadata rebuild ([`Repository::push`](crate::repo::Repository)
/// after a commit, [`Repository::compute_rollup`](crate::repo::Repository))
/// constructs a *fresh* branch-head tribleset from
/// [`branch_metadata`](crate::repo::branch::branch_metadata) and would
/// otherwise drop the manifest that lived in the previous head. Unioning the
/// result of this function back into the freshly built head carries the LSMT
/// manifest forward across the rebuild: segments then **accumulate across
/// commits** (rather than each rebuild resetting the manifest to empty and
/// forcing the next [`IndexHome::update_index`] to start a single fresh
/// segment), so the size-tiered merge and reachability GC run on the live
/// cadence.
///
/// The manifest stays ephemeral: it is still redundant with the commit chain
/// and re-derivable by [`IndexHome::update_index`]; this only changes the
/// rebuild *cadence* from wipe-every-commit to carry-and-compact. The segment
/// blobs it references remain reachable from (and GC-able via) the branch
/// head, exactly as before.
///
/// Each kind's manifest is round-tripped through
/// [`Manifest::from_tribles`]/[`Manifest::to_tribles`], which reproduces the
/// content-addressed segment entities verbatim (the same fidelity
/// [`IndexHome::update_index`] relies on for its manifest difference).
pub(crate) fn manifest_tribles(set: &TribleSet) -> TribleSet {
    // Enumerate the distinct owning kinds present in the head, then carry
    // each kind's manifest forward independently (two kinds over one branch
    // keep independent manifests in the one tribleset).
    let mut kinds: Vec<Id> = Vec::new();
    for (k,) in find!(
        (k: Inline<GenId>),
        pattern!(set, [{ _?e @ seg_kind: ?k }])
    ) {
        if let Ok(raw) = k.try_from_inline::<crate::id::RawId>() {
            if let Some(id) = Id::new(raw) {
                if !kinds.contains(&id) {
                    kinds.push(id);
                }
            }
        }
    }

    let mut out = TribleSet::new();
    for kind in kinds {
        out += Manifest::from_tribles(set, kind).to_tribles(kind);
    }
    out
}

/// Error surfaced by the [`IndexHome`] surface.
#[derive(Debug)]
pub enum IndexError {
    /// An underlying storage operation failed.
    Storage(Box<dyn Error + Send + Sync>),
    /// A manifest referenced a segment that could not be decoded as this
    /// index kind. The manifest is repairable soft state.
    Segment(Box<dyn Error + Send + Sync>),
    /// The branch pin advanced between read and CAS-write (a concurrent
    /// commit or another index update). The caller may retry — segment
    /// blobs are content-addressed, so a retry dedupes against
    /// already-uploaded blobs.
    Conflict,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Storage(e) => write!(f, "index-home storage error: {e}"),
            IndexError::Segment(e) => write!(f, "index-home segment decode error: {e}"),
            IndexError::Conflict => write!(f, "index-home manifest pin advanced concurrently"),
        }
    }
}

impl Error for IndexError {}

fn boxed<E: Error + Send + Sync + 'static>(e: E) -> IndexError {
    IndexError::Storage(Box::new(e))
}

/// Build one level-0 segment for `kind` from `source` and fold the
/// updated manifest into `head_set` (a branch-head tribleset under
/// construction), running the size-tiered merge to keep fan-out bounded.
///
/// This is the shared core of both maintenance triggers: the explicit
/// [`IndexHome::update_index`] (which reads the head, applies this, and
/// CAS-repoints the branch pin) and the on-commit hook installed by
/// [`Repository::register_index`](crate::repo::Repository::register_index)
/// (which applies this to the head tribleset a push is about to CAS in,
/// so commit and index maintenance land in one atomic repoint).
///
/// Segment (and stats) blobs are uploaded to `storage` immediately; if
/// the caller's CAS later loses, they are unreferenced, content-addressed
/// blobs — a retry dedupes against them and GC reclaims them otherwise.
/// `head_set` is only replaced on success (`Err` leaves it untouched).
pub fn append_segment<S, K>(
    storage: &mut S,
    kind: &K,
    source: &TribleSet,
    head_set: &mut TribleSet,
) -> Result<(), IndexError>
where
    S: BlobStore,
    K: IndexKind,
{
    let kind_id = kind.kind_id();
    let mut manifest = Manifest::from_tribles(head_set, kind_id);
    // Build + append a fresh level-0 segment.
    let segment_blob = kind.build(source);
    let handle = storage.put::<UnknownBlob, _>(segment_blob).map_err(boxed)?;
    manifest.push(handle, 0);

    // Size-tiered merge: while a level overflows, tenure the whole
    // tier into a single merged segment one level up (Yard's shape).
    while let Some(level) = manifest.overflowing_level(FANOUT) {
        let victims = manifest.drain_level(level);
        let reader = storage.reader().map_err(boxed)?;
        let mut attached = Vec::with_capacity(victims.len());
        for v in &victims {
            let blob: Blob<UnknownBlob> = reader.get(v.blob).map_err(boxed)?;
            attached.push(kind.try_attach(blob).map_err(IndexError::Segment)?);
        }
        let merged_blob = kind.merge(&attached);
        let merged_handle = storage.put::<UnknownBlob, _>(merged_blob).map_err(boxed)?;
        manifest.push(merged_handle, level + 1);
    }

    // Rewrite this kind's manifest inside the branch-head tribleset:
    // drop the old segment tribles, union the new ones in. All other
    // tribles (branch metadata, other kinds' segments) are preserved
    // because union only adds and the difference targets exactly this
    // kind's old segment entities.
    replace_manifest(head_set, kind_id, &manifest);
    Ok(())
}

/// The index-home surface for one `(source_branch, kind)`: reads and
/// maintains the LSMT of segments whose manifest lives as tribles inside
/// the branch-head tribleset.
///
/// Generic over any storage that is both a [`BlobStore`] (segment blobs +
/// the branch-head tribleset blob) and a [`PinStore`] (the mutable branch
/// pin). The manifest is unioned into the branch-head tribleset, so GC of
/// orphaned segments is the store's own reachability sweep — the segment
/// handles are values in the branch head, which is already a reachability
/// root — see [`Yard`](crate::repo::yard::Yard). No separate pin.
pub struct IndexHome<'s, S, K> {
    storage: &'s mut S,
    kind: K,
    /// The branch pin id whose head tribleset carries this kind's manifest.
    branch: Id,
}

impl<'s, S, K> IndexHome<'s, S, K>
where
    S: BlobStore + PinStore,
    K: IndexKind,
{
    /// Open the index home for `kind` over `source_branch`, backed by
    /// `storage`. The manifest lives in `source_branch`'s head tribleset.
    /// Does not touch storage until a read or update.
    pub fn new(storage: &'s mut S, source_branch: Id, kind: K) -> Self {
        Self {
            storage,
            kind,
            branch: source_branch,
        }
    }

    fn head(&mut self) -> Result<Option<Inline<Handle<SimpleArchive>>>, IndexError> {
        self.storage.head(self.branch).map_err(boxed)
    }

    /// Read the current branch-head tribleset (empty if the pin is unset).
    fn head_set(&mut self) -> Result<TribleSet, IndexError> {
        match self.head()? {
            None => Ok(TribleSet::new()),
            Some(handle) => {
                let reader = self.storage.reader().map_err(boxed)?;
                reader.get(handle).map_err(boxed)
            }
        }
    }

    /// Read the current manifest (empty if the index has no segments yet).
    pub fn read_manifest(&mut self) -> Result<Manifest, IndexError> {
        let set = self.head_set()?;
        Ok(Manifest::from_tribles(&set, self.kind.kind_id()))
    }

    /// Attach every live segment named by the manifest, ready for a
    /// union query. No checkout, no commit walk — one manifest lookup
    /// plus a bounded number of segment fetches.
    pub fn attach_all(&mut self) -> Result<Vec<K::Segment>, IndexError> {
        let manifest = self.read_manifest()?;
        self.attach_manifest(&manifest)
    }

    /// Attach the segments from an already-read manifest snapshot.
    ///
    /// This lets a caller read one branch-head tribleset, validate its source
    /// coverage, and then attach exactly the segment handles certified by that
    /// same snapshot. Unlike [`attach_all`](Self::attach_all), this method does
    /// not reread the mutable branch pin.
    pub fn attach_manifest(&mut self, manifest: &Manifest) -> Result<Vec<K::Segment>, IndexError> {
        let reader = self.storage.reader().map_err(boxed)?;
        let mut out = Vec::with_capacity(manifest.segments.len());
        for e in &manifest.segments {
            let blob: Blob<UnknownBlob> = reader.get(e.blob).map_err(boxed)?;
            out.push(self.kind.try_attach(blob).map_err(IndexError::Segment)?);
        }
        Ok(out)
    }

    /// The explicit maintenance entry point (the on-commit twin is
    /// [`Repository::register_index`](crate::repo::Repository::register_index),
    /// which runs [`append_segment`] inside the push itself).
    ///
    /// Builds a new level-0 segment from `source` (a bounded delta),
    /// runs a size-tiered merge to bound fan-out, and CAS-repoints the
    /// branch pin at a new branch-head tribleset: this kind's old segment
    /// tribles differenced out, the new ones unioned in — every other
    /// trible (branch metadata, other kinds' segments) preserved verbatim.
    /// Superseded segments and the old branch-head tribleset become
    /// unreachable and are reclaimed by the store's GC.
    ///
    /// Because `source` is an arbitrary caller-supplied view rather than a
    /// commit batch, this operation clears any source-commit coverage
    /// certificate. Callers that know and have verified a frontier can publish
    /// it separately in the same higher-level transaction.
    ///
    /// Returns [`IndexError::Conflict`] if the branch pin advanced
    /// concurrently; the caller may retry.
    pub fn update_index(&mut self, source: &TribleSet) -> Result<(), IndexError> {
        let old_head = self.head()?;
        let mut new_set = self.head_set()?;
        append_segment(&mut *self.storage, &self.kind, source, &mut new_set)?;
        set_coverage(&mut new_set, self.kind.kind_id(), Vec::new());
        let new_head: Inline<Handle<SimpleArchive>> = self.storage.put(new_set).map_err(boxed)?;
        match self
            .storage
            .update(self.branch, old_head, Some(new_head))
            .map_err(boxed)?
        {
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
/// - `merge` — **CPU** structural merge: remap segment-local domains, k-way
///   merge all six sorted rotations, and rebuild the wavelet structures
///   without reconstructing the full six-PATCH [`TribleSet`]. This remains
///   the clean seam where a GPU implementation can drop in without changing
///   the surface.
#[derive(Debug, Clone, Copy, Default)]
pub struct SuccinctRollup;

impl SuccinctRollup {
    /// Stable kind id of the SuccinctArchive rollup (minted via
    /// `trible genid`), tagged onto every segment entity via [`seg_kind`].
    /// Mirrors the `KIND_ID_HEX` consts on the BM25 and HNSW kinds in
    /// `triblespace-search`.
    pub const KIND_ID_HEX: &'static str = "9540D50DEDECA9CA948FD14474F86566";

    /// Construct the SuccinctArchive rollup kind.
    pub fn new() -> Self {
        SuccinctRollup
    }

    /// A queryable view over a set of attached segments that unions them
    /// into one logical dataset — the correct LSMT read (a single match
    /// may span segments).
    pub fn union<'a>(
        segments: &'a [SuccinctArchive<OrderedUniverse>],
    ) -> UnionArchive<'a, OrderedUniverse> {
        UnionArchive::new(segments)
    }
}

impl IndexKind for SuccinctRollup {
    type Segment = SuccinctArchive<OrderedUniverse>;

    fn kind_id(&self) -> Id {
        Id::from_hex(Self::KIND_ID_HEX).expect("valid kind id")
    }

    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
        let archive: SuccinctArchive<OrderedUniverse> = source.into();
        {
            let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
            blob.transmute()
        }
    }

    fn try_attach(
        &self,
        blob: Blob<UnknownBlob>,
    ) -> Result<Self::Segment, Box<dyn Error + Send + Sync>> {
        blob.transmute::<SuccinctArchiveBlob>()
            .try_from_blob()
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        // Merge the six already-sorted Ring rotations directly. Domains are
        // remapped and wavelet matrices rebuilt, but no six-index TribleSet is
        // reconstructed at the compaction tier's full size.
        let archive = merge_ordered_archives(segments);
        {
            let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
            blob.transmute()
        }
    }
}

/// Succinct rollup that delegates sufficiently large wavelet-freeze passes to
/// an external accelerator backend while preserving the same kind id, segment
/// format, and canonical bytes as [`SuccinctRollup`].
///
/// Small compactions stay on the CPU because device dispatch dominates below
/// the backend's measured break-even. If the accelerator returns an error, the
/// merge retries on the canonical CPU implementation and disables further
/// accelerator attempts until [`reset_accelerator`](Self::reset_accelerator)
/// is called. This fallback handles returned errors only: it does not catch a
/// backend panic, allocation failure, abort, or device loss that terminates the
/// process.
pub struct AcceleratedSuccinctRollup<B> {
    backend: B,
    min_input_rows: usize,
    accelerator_enabled: AtomicBool,
}

impl<B> AcceleratedSuccinctRollup<B> {
    /// Construct an accelerated rollup.
    ///
    /// `min_input_rows` is compared with the saturating sum of the input
    /// segments' row counts, before cross-segment duplicates are removed. A
    /// deployment should therefore calibrate the threshold with that same
    /// quantity rather than with the deduplicated output size.
    pub fn new(backend: B, min_input_rows: usize) -> Self {
        Self {
            backend,
            min_input_rows,
            accelerator_enabled: AtomicBool::new(true),
        }
    }

    /// Borrow the configured accelerator backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Return the configured input-row CPU/accelerator crossover threshold.
    pub fn min_input_rows(&self) -> usize {
        self.min_input_rows
    }

    /// Whether the accelerator circuit is currently closed (enabled).
    pub fn accelerator_enabled(&self) -> bool {
        self.accelerator_enabled.load(Ordering::Relaxed)
    }

    /// Re-enable accelerator attempts after an operator has repaired or
    /// replaced the backend.
    pub fn reset_accelerator(&self) {
        self.accelerator_enabled.store(true, Ordering::Relaxed);
    }
}

impl<B> IndexKind for AcceleratedSuccinctRollup<B>
where
    B: WaveletMatrixFreezeBackend,
{
    type Segment = SuccinctArchive<OrderedUniverse>;

    fn kind_id(&self) -> Id {
        SuccinctRollup.kind_id()
    }

    fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
        SuccinctRollup.build(source)
    }

    fn try_attach(
        &self,
        blob: Blob<UnknownBlob>,
    ) -> Result<Self::Segment, Box<dyn Error + Send + Sync>> {
        SuccinctRollup.try_attach(blob)
    }

    fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
        let input_rows = segments.iter().fold(0usize, |sum, segment| {
            sum.saturating_add(segment.eav_c.len())
        });
        if input_rows >= self.min_input_rows && self.accelerator_enabled() {
            match merge_ordered_archives_with_backend(segments, &self.backend) {
                Ok(archive) => {
                    let blob: Blob<SuccinctArchiveBlob> = (&archive).to_blob();
                    return blob.transmute();
                }
                Err(_) => {
                    self.accelerator_enabled.store(false, Ordering::Relaxed);
                }
            }
        }
        SuccinctRollup.merge(segments)
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
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'p> {
        let e: Term<GenId> = e.into();
        let a: Term<GenId> = a.into();
        let v: Term<V> = v.into();
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use super::*;
    use crate::examples::literature;
    use crate::id::fucid;
    use crate::inline::TryToInline;
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::BlobStorePut;

    fn person(name: &str) -> TribleSet {
        let id = fucid();
        entity! { &id @ literature::firstname: name }.into()
    }

    // A trivial branch pin id. For the mechanical LSMT tests the pin
    // starts unset (empty head tribleset) and index-home unions the
    // manifest tribles straight into it; the coexistence test below uses a
    // real branch pin that already carries branch metadata.
    fn source_branch() -> Id {
        *fucid()
    }

    #[test]
    fn manifest_roundtrips_through_tribles() {
        let mut m = Manifest::default();
        let h0 = Inline::<Handle<UnknownBlob>>::new([1u8; 32]);
        let h1 = Inline::<Handle<UnknownBlob>>::new([2u8; 32]);
        let tip = Inline::<Handle<SimpleArchive>>::new([3u8; 32]);
        let other_tip = Inline::<Handle<SimpleArchive>>::new([4u8; 32]);
        m.push(h0, 0);
        m.push(h1, 1);
        m.set_covered(vec![other_tip, tip, tip]);

        let kind = SuccinctRollup.kind_id();
        let parsed = Manifest::from_tribles(&m.to_tribles(kind), kind);
        assert_eq!(parsed.segments.len(), 2);
        // Ordered by (level, seq).
        assert_eq!(parsed.segments[0].blob, h0);
        assert_eq!(parsed.segments[0].level, 0);
        assert_eq!(parsed.segments[1].blob, h1);
        assert_eq!(parsed.segments[1].level, 1);
        // next_seq continues past the max seq read back.
        assert_eq!(parsed.next_seq, 2);
        assert_eq!(parsed.covered, vec![tip, other_tip]);
        assert!(!parsed.covers_head(Some(tip)));
        assert_eq!(
            parsed.to_tribles(kind),
            m.to_tribles(kind),
            "canonical coverage entities round-trip exactly"
        );
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
            assert_eq!(home.read_manifest().unwrap().segments.len(), 1);
        }
        let first_head = storage.head(branch).unwrap();

        {
            let mut home = IndexHome::new(&mut storage, branch, SuccinctRollup::new());
            home.update_index(&db).unwrap();
            let m = home.read_manifest().unwrap();
            // FANOUT is 4, so two updates leave two level-0 segments.
            assert_eq!(m.segments.len(), 2);
        }
        let second_head = storage.head(branch).unwrap();

        // The branch pin now names the newest branch-head tribleset, not
        // the first.
        assert_ne!(first_head, second_head);
    }

    #[test]
    fn explicit_update_invalidates_commit_coverage() {
        let mut storage = MemoryRepo::default();
        let branch = source_branch();
        let kind = SuccinctRollup::new();
        let certified = Inline::<Handle<SimpleArchive>>::new([9u8; 32]);

        {
            let mut home = IndexHome::new(&mut storage, branch, kind);
            home.update_index(&person("Ada")).unwrap();
        }
        let old_head = storage.head(branch).unwrap().unwrap();
        let mut head_set: TribleSet = storage.reader().unwrap().get(old_head).unwrap();
        set_coverage(&mut head_set, kind.kind_id(), vec![certified]);
        let certified_head: Inline<Handle<SimpleArchive>> = storage.put(head_set).unwrap();
        assert!(matches!(
            storage
                .update(branch, Some(old_head), Some(certified_head))
                .unwrap(),
            PushResult::Success()
        ));

        let mut home = IndexHome::new(&mut storage, branch, kind);
        assert!(home.read_manifest().unwrap().covers_head(Some(certified)));
        home.update_index(&person("Grace")).unwrap();
        assert!(
            home.read_manifest().unwrap().covered.is_empty(),
            "an arbitrary explicit source cannot retain a commit certificate"
        );
    }

    #[test]
    fn union_into_branch_head_preserves_unrelated_queries() {
        // JP's core point, proven directly: index-manifest tribles union
        // into a branch-head tribleset that ALSO holds unrelated
        // (branch-metadata) tribles. Afterwards (a) an index query reads
        // exactly the manifest subset and (b) an unrelated query over the
        // branch-metadata attributes returns identical results with the
        // index tribles present as it did without them — union only adds,
        // so existing subset queries don't change.
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch = repo.create_branch("main", None).unwrap();

        // Commit real content so the branch-head tribleset carries genuine
        // branch metadata (name / head / signature / updated_at).
        let mut ws = repo.pull(*branch).unwrap();
        ws.commit(person("Ada"), "seed");
        repo.push(&mut ws).unwrap();

        // Snapshot the branch-head tribleset BEFORE any index tribles, and
        // run an unrelated query over the branch-metadata subset.
        let head_before = repo.storage_mut().head(*branch).unwrap().unwrap();
        let set_before: TribleSet = repo
            .storage_mut()
            .reader()
            .unwrap()
            .get(head_before)
            .unwrap();
        let names_before: Vec<_> = find!(
            (n: Inline<_>),
            pattern!(&set_before, [{ crate::metadata::name: ?n }])
        )
        .collect();
        // No index manifest present yet.
        assert_eq!(
            Manifest::from_tribles(&set_before, SuccinctRollup.kind_id())
                .segments
                .len(),
            0
        );

        // Union index-manifest tribles into the SAME branch-head tribleset.
        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            home.update_index(&person("Grace")).unwrap();
        }

        let head_after = repo.storage_mut().head(*branch).unwrap().unwrap();
        assert_ne!(
            head_before, head_after,
            "branch pin repointed to the new tribleset"
        );
        let set_after: TribleSet = repo
            .storage_mut()
            .reader()
            .unwrap()
            .get(head_after)
            .unwrap();

        // (a) The index query reads exactly the manifest subset.
        let manifest = Manifest::from_tribles(&set_after, SuccinctRollup.kind_id());
        assert_eq!(
            manifest.segments.len(),
            1,
            "index query sees exactly its one segment"
        );

        // (b) The unrelated branch-metadata query is IDENTICAL with the
        // index tribles present — union didn't disturb the existing subset.
        let names_after: Vec<_> = find!(
            (n: Inline<_>),
            pattern!(&set_after, [{ crate::metadata::name: ?n }])
        )
        .collect();
        assert_eq!(
            names_before, names_after,
            "union doesn't change existing subset queries"
        );
        assert_eq!(names_after.len(), 1);

        // The branch's committed content is untouched: the index segment
        // (Grace) rode into the branch-head *metadata* tribleset without
        // being committed to the branch. A checkout sees only Ada.
        let checkout = repo.pull(*branch).unwrap().checkout(..).unwrap();
        let people: Vec<_> = find!(
            (n: Inline<_>),
            pattern!(&*checkout, [{ _?p @ literature::firstname: ?n }])
        )
        .collect();
        assert_eq!(
            people.len(),
            1,
            "branch content intact; index segment not committed"
        );
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
        assert_eq!(m.segments.len(), 2, "expected two independent segments");

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

    fn reference_freeze(sequence: &[u32], planes: &mut [&mut [u64]]) {
        let mut current = sequence.to_vec();
        let mut next = vec![0u32; sequence.len()];
        let width = planes.len();
        for (depth, plane) in planes.iter_mut().enumerate() {
            plane.fill(0);
            let shift = width - 1 - depth;
            let mut zeros = 0usize;
            for (position, &code) in current.iter().enumerate() {
                let bit = (code >> shift) & 1;
                if bit == 0 {
                    zeros += 1;
                } else {
                    plane[position / 64] |= 1u64 << (position % 64);
                }
            }
            let (mut zero, mut one) = (0usize, zeros);
            for &code in &current {
                if (code >> shift) & 1 == 0 {
                    next[zero] = code;
                    zero += 1;
                } else {
                    next[one] = code;
                    one += 1;
                }
            }
            std::mem::swap(&mut current, &mut next);
        }
    }

    struct CountingFreeze {
        calls: AtomicUsize,
        fail: bool,
    }

    impl CountingFreeze {
        fn succeeding() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail: true,
            }
        }
    }

    impl WaveletMatrixFreezeBackend for CountingFreeze {
        type Error = ();

        fn freeze_rotation(
            &self,
            _rotation: crate::blob::encodings::succinctarchive::SuccinctRotation,
            _alphabet_size: usize,
            sequence: &[u32],
            planes: &mut [&mut [u64]],
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            if self.fail {
                return Err(());
            }
            reference_freeze(sequence, planes);
            Ok(())
        }
    }

    #[test]
    fn accelerated_rollup_threshold_is_inclusive_input_row_sum() {
        let cpu = SuccinctRollup::new();
        let da = person("Ada");
        let db = person("Grace");
        let segments = [cpu.attach(cpu.build(&da)), cpu.attach(cpu.build(&db))];
        let input_rows = segments
            .iter()
            .map(|segment| segment.eav_c.len())
            .sum::<usize>();
        let expected = cpu.merge(&segments);

        for (threshold, expected_calls) in [
            (input_rows + 1, 0),
            (input_rows, 6),
            (input_rows.saturating_sub(1), 6),
        ] {
            let accelerated =
                AcceleratedSuccinctRollup::new(CountingFreeze::succeeding(), threshold);
            let actual = accelerated.merge(&segments);
            assert_eq!(actual.bytes.as_ref(), expected.bytes.as_ref());
            assert_eq!(
                accelerated.backend().calls.load(AtomicOrdering::Relaxed),
                expected_calls,
                "threshold {threshold} for {input_rows} input rows"
            );
            assert!(accelerated.accelerator_enabled());
            assert_eq!(accelerated.min_input_rows(), threshold);
        }
    }

    #[test]
    fn accelerated_rollup_circuit_breaks_after_returned_failure() {
        let cpu = SuccinctRollup::new();
        let accelerated = AcceleratedSuccinctRollup::new(CountingFreeze::failing(), 0);
        let da = person("Ada");
        let db = person("Grace");
        let segments = [cpu.attach(cpu.build(&da)), cpu.attach(cpu.build(&db))];

        let expected = cpu.merge(&segments);
        for _ in 0..2 {
            let actual = accelerated.merge(&segments);
            assert_eq!(actual.bytes.as_ref(), expected.bytes.as_ref());
        }
        assert_eq!(accelerated.kind_id(), cpu.kind_id());
        assert!(!accelerated.accelerator_enabled());
        assert_eq!(
            accelerated.backend().calls.load(AtomicOrdering::Relaxed),
            1,
            "the second merge must skip the failed backend"
        );

        accelerated.reset_accelerator();
        assert!(accelerated.accelerator_enabled());
        let actual = accelerated.merge(&segments);
        assert_eq!(actual.bytes.as_ref(), expected.bytes.as_ref());
        assert_eq!(accelerated.backend().calls.load(AtomicOrdering::Relaxed), 2);
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
            assert!(m.segments.len() < names.len());
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
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
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
    fn segments_accumulate_across_rollup_and_commit_cycles() {
        // The keystone invariant: a `compute_rollup` rebuild AND a commit
        // (`push`) must both carry the existing manifest forward, so segments
        // ACCUMULATE across cycles instead of the branch-head rebuild wiping
        // them (which would force every update_index to start a fresh single
        // segment and keep the tiering/merge/GC from ever firing live).
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch = repo.create_branch("main", None).unwrap();

        // Seed a real commit so compute_rollup has a HEAD to roll up.
        let mut ws = repo.pull(*branch).unwrap();
        ws.commit(person("seed"), "seed");
        repo.push(&mut ws).unwrap();

        // Cycle 1: append a segment, then rebuild the rollup.
        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            home.update_index(&person("Ada")).unwrap();
            assert_eq!(home.read_manifest().unwrap().segments.len(), 1);
        }
        repo.compute_rollup(*branch).unwrap();
        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            assert_eq!(
                home.read_manifest().unwrap().segments.len(),
                1,
                "compute_rollup must carry the existing segment forward, not drop it"
            );
        }

        // A commit (push) between updates must also preserve the manifest —
        // this is the literal "accumulate across commits" invariant.
        let mut ws = repo.pull(*branch).unwrap();
        ws.commit(person("interleaved-commit"), "commit");
        repo.push(&mut ws).unwrap();
        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            assert_eq!(
                home.read_manifest().unwrap().segments.len(),
                1,
                "a commit must carry the manifest forward, not wipe it"
            );
        }

        // Cycle 2: append a second segment, rebuild the rollup again.
        {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            home.update_index(&person("Grace")).unwrap();
            assert_eq!(home.read_manifest().unwrap().segments.len(), 2);
        }
        repo.compute_rollup(*branch).unwrap();

        // Both segments survive across the two rollup cycles and the commit;
        // they did NOT collapse to a single per-import segment.
        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let manifest = home.read_manifest().unwrap();
        assert_eq!(
            manifest.segments.len(),
            2,
            "both segments accumulate across rollup cycles; not collapsed to one"
        );
        // Two distinct level-0 segments (FANOUT=4, no merge yet).
        assert!(manifest.segments.iter().all(|s| s.level == 0));

        // Both appended facts remain queryable through the union read.
        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        let mut names: Vec<_> = find!(
            (name: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?name }])
        )
        .collect();
        names.sort();
        assert_eq!(names.len(), 2, "both appended segments remain queryable");
    }

    #[test]
    fn gc_reclaims_orphaned_segments() {
        // Drive the index over a Yard so the store's reachability GC runs.
        // After enough updates to trigger a merge, the merged-away level-0
        // segments are unreachable from the branch-head tribleset (the
        // branch pin's reachability root) and must be reclaimed, while the
        // live merged segment survives and the union
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
            assert_eq!(m.segments.len(), 1);
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

    #[test]
    fn on_commit_hook_maintains_index_incrementally() {
        // The on-commit trigger: register the kind once, then just
        // commit. Every push builds one level-0 segment from that push's
        // content delta and folds the manifest into the same head CAS —
        // no explicit update_index, no checkout, ever.
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        repo.register_index(SuccinctRollup::new());
        let branch = repo.create_branch("main", None).unwrap();

        // Enough commits to overflow a level (FANOUT = 4) and fire the
        // size-tiered merge, plus a few more on top of the merged tier.
        let n = 2 * FANOUT - 1;
        for i in 0..n {
            let mut ws = repo.pull(*branch).unwrap();
            ws.commit(person(&format!("p{i}")), "c");
            repo.push(&mut ws).unwrap();

            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            let m = home.read_manifest().unwrap();
            if i + 1 < FANOUT {
                // One level-0 segment per commit until the merge fires.
                assert_eq!(
                    m.segments.len(),
                    i + 1,
                    "segment per commit before first merge"
                );
                assert!(m.segments.iter().all(|s| s.level == 0));
            }
            // Fan-out stays bounded at every step.
            let mut per_level: HashMap<u64, usize> = HashMap::new();
            for e in &m.segments {
                *per_level.entry(e.level).or_default() += 1;
            }
            assert!(per_level.values().all(|&c| c < FANOUT), "fan-out bounded");
        }

        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let m = home.read_manifest().unwrap();
        // The tiered merge fired: fewer segments than commits, and a
        // tenured segment above level 0 exists.
        assert!(
            m.segments.len() < n,
            "merge collapsed level 0 at least once"
        );
        assert!(
            m.segments.iter().any(|s| s.level > 0),
            "tenured segment exists"
        );

        // The union read sees ALL committed data — attach the manifest's
        // segments straight from the branch head, no checkout involved.
        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        let count = find!(
            (name: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?name }])
        )
        .count();
        assert_eq!(count, n, "union read covers every commit's delta");
        assert!(
            m.covers_head(repo.pull(*branch).unwrap().head()),
            "coverage certificate follows source HEAD"
        );
        assert!(repo.take_hook_errors().is_empty());
    }

    #[test]
    fn several_commits_in_one_push_become_distinct_leaves() {
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        repo.register_index(SuccinctRollup::new());
        let branch = repo.create_branch("main", None).unwrap();

        let mut ws = repo.pull(*branch).unwrap();
        for name in ["Ada", "Grace", "Edsger"] {
            ws.commit(person(name), "person");
        }
        let pushed_head = ws.head().unwrap();
        repo.push(&mut ws).unwrap();
        assert!(repo.take_hook_errors().is_empty());

        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let manifest = home.read_manifest().unwrap();
        assert_eq!(
            manifest.segments.len(),
            3,
            "one physical L0 leaf per content-bearing commit, not per push"
        );
        assert!(manifest.segments.iter().all(|entry| entry.level == 0));
        assert_eq!(manifest.covered, vec![pushed_head]);

        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        assert_eq!(
            find!(
                (name: Inline<_>),
                pattern!(&union, [{ _?person @ literature::firstname: ?name }])
            )
            .count(),
            3
        );
    }

    #[test]
    fn two_kinds_coexist_each_selecting_its_own_subset() {
        // Two kinds registered simultaneously: both manifests coexist in
        // the one branch-head tribleset, and each kind's segments carry
        // only its own (filtered) source subset.
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        /// Same segment format as [`SuccinctRollup`], distinct kind id
        /// (minted via `trible genid`).
        struct TitlesRollup;
        impl IndexKind for TitlesRollup {
            type Segment = SuccinctArchive<OrderedUniverse>;
            fn kind_id(&self) -> Id {
                Id::from_hex("BF75EE8DE0B85E72B895AB0726941AAE").expect("valid kind id")
            }
            fn build(&self, source: &TribleSet) -> Blob<UnknownBlob> {
                SuccinctRollup.build(source)
            }
            fn try_attach(
                &self,
                blob: Blob<UnknownBlob>,
            ) -> Result<Self::Segment, Box<dyn Error + Send + Sync>> {
                SuccinctRollup.try_attach(blob)
            }
            fn merge(&self, segments: &[Self::Segment]) -> Blob<UnknownBlob> {
                SuccinctRollup.merge(segments)
            }
        }

        fn keep(attr: Id) -> impl FnMut(&TribleSet) -> TribleSet + Send + Sync + 'static {
            move |delta: &TribleSet| {
                let mut out = TribleSet::new();
                for t in delta.iter().filter(|t| *t.a() == attr) {
                    out.insert(t);
                }
                out
            }
        }

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        repo.register_index_filtered(SuccinctRollup::new(), keep(literature::firstname.id()));
        repo.register_index_filtered(TitlesRollup, keep(literature::title.id()));
        let branch = repo.create_branch("main", None).unwrap();

        let n = 2;
        for i in 0..n {
            let mut ws = repo.pull(*branch).unwrap();
            let id = fucid();
            let name = format!("n{i}");
            let title = format!("t{i}");
            ws.commit(
                entity! { &id @ literature::firstname: name.as_str(), literature::title: title.as_str() },
                "c",
            );
            repo.push(&mut ws).unwrap();
        }
        assert!(repo.take_hook_errors().is_empty());

        // Both manifests coexist at the head, one per kind.
        let head = repo.storage_mut().head(*branch).unwrap().unwrap();
        let head_set: TribleSet = repo.storage_mut().reader().unwrap().get(head).unwrap();
        assert_eq!(
            Manifest::from_tribles(&head_set, SuccinctRollup.kind_id())
                .segments
                .len(),
            n
        );
        assert_eq!(
            Manifest::from_tribles(&head_set, TitlesRollup.kind_id())
                .segments
                .len(),
            n
        );

        // Each kind's union read selects exactly its own subset.
        let names_union_counts = {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
            let segments = home.attach_all().unwrap();
            let union = SuccinctRollup::union(&segments);
            let names = find!(
                (v: Inline<_>),
                pattern!(&union, [{ _?p @ literature::firstname: ?v }])
            )
            .count();
            let titles = find!(
                (v: Inline<_>),
                pattern!(&union, [{ _?p @ literature::title: ?v }])
            )
            .count();
            (names, titles)
        };
        assert_eq!(
            names_union_counts,
            (n, 0),
            "firstname kind carries only names"
        );

        let titles_union_counts = {
            let mut home = IndexHome::new(repo.storage_mut(), *branch, TitlesRollup);
            let segments = home.attach_all().unwrap();
            let union = SuccinctRollup::union(&segments);
            let names = find!(
                (v: Inline<_>),
                pattern!(&union, [{ _?p @ literature::firstname: ?v }])
            )
            .count();
            let titles = find!(
                (v: Inline<_>),
                pattern!(&union, [{ _?p @ literature::title: ?v }])
            )
            .count();
            (names, titles)
        };
        assert_eq!(
            titles_union_counts,
            (0, n),
            "title kind carries only titles"
        );
    }

    #[test]
    fn hook_error_is_skipped_and_commit_lands() {
        // The failure policy: a hook error must neither block nor corrupt
        // the commit. The failing hook contributes nothing, later hooks
        // still run, the push succeeds, and the error is drainable.
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        repo.on_commit(|_storage, _branch, _delta, _head| Err("boom".into()));
        repo.register_index(SuccinctRollup::new());
        let branch = repo.create_branch("main", None).unwrap();

        let mut ws = repo.pull(*branch).unwrap();
        ws.commit(person("Ada"), "c");
        repo.push(&mut ws).unwrap();

        // The commit landed.
        let checkout = repo.pull(*branch).unwrap().checkout(..).unwrap();
        let committed = find!(
            (v: Inline<_>),
            pattern!(&*checkout, [{ _?p @ literature::firstname: ?v }])
        )
        .count();
        assert_eq!(committed, 1, "commit lands despite the failing hook");

        // The index hook registered after the failing one still ran.
        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        assert_eq!(home.read_manifest().unwrap().segments.len(), 1);

        // The failure is recorded once and drained.
        let errors = repo.take_hook_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].branch, *branch);
        assert_eq!(errors[0].error.to_string(), "boom");
        assert!(repo.take_hook_errors().is_empty(), "drained");
    }

    #[test]
    fn hooks_rerun_per_attempt_on_conflict() {
        // A conflicting push goes through merge-and-retry; the hook runs
        // once per attempt against that attempt's delta. Content-addressed
        // segments make the re-run idempotent, and the losing attempt's
        // head mutation is discarded with its CAS — so the final index is
        // exactly one segment per landed content delta.
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        repo.register_index(SuccinctRollup::new());
        let branch = repo.create_branch("main", None).unwrap();

        // Two workspaces pulled from the same base; the second push
        // conflicts and retries through a merge commit.
        let mut ws1 = repo.pull(*branch).unwrap();
        let mut ws2 = repo.pull(*branch).unwrap();
        ws1.commit(person("Ada"), "a");
        repo.push(&mut ws1).unwrap();
        ws2.commit(person("Grace"), "b");
        repo.push(&mut ws2).unwrap();
        assert!(repo.take_hook_errors().is_empty());

        // Both facts are reachable through the union read.
        let mut home = IndexHome::new(repo.storage_mut(), *branch, SuccinctRollup::new());
        let manifest = home.read_manifest().unwrap();
        assert_eq!(manifest.segments.len(), 2, "one segment per landed delta");
        assert_eq!(manifest.covered, vec![ws2.head().unwrap()]);
        let segments = home.attach_all().unwrap();
        let union = SuccinctRollup::union(&segments);
        let mut names: Vec<_> = find!(
            (v: Inline<_>),
            pattern!(&union, [{ _?p @ literature::firstname: ?v }])
        )
        .collect();
        names.sort();
        assert_eq!(names.len(), 2, "both writers' deltas indexed");
    }

    #[test]
    fn an_unhooked_gap_stays_detectably_stale() {
        use crate::repo::Repository;
        use ed25519_dalek::SigningKey;

        let key = SigningKey::from_bytes(&[11u8; 32]);
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(storage, key.clone(), TribleSet::new()).unwrap();
        repo.register_index(SuccinctRollup::new());
        let branch = repo.create_branch("main", None).unwrap();

        let mut first = repo.pull(*branch).unwrap();
        first.commit(person("indexed"), "indexed");
        repo.push(&mut first).unwrap();
        let indexed_head = first.head().unwrap();
        assert!(repo.take_hook_errors().is_empty());

        // Reopen without process-local hooks and land one source commit. The
        // manifest remains pinned to the earlier source HEAD.
        let storage = repo.into_storage();
        let mut unhooked = Repository::new(storage, key.clone(), TribleSet::new()).unwrap();
        let mut missed = unhooked.pull(*branch).unwrap();
        missed.commit(person("missed"), "missed");
        unhooked.push(&mut missed).unwrap();
        let missed_head = missed.head().unwrap();

        // Reinstall maintenance and push again. It must not skip over the
        // missed commit and falsely certify the new HEAD.
        let storage = unhooked.into_storage();
        let mut repaired = Repository::new(storage, key, TribleSet::new()).unwrap();
        repaired.register_index(SuccinctRollup::new());
        let mut later = repaired.pull(*branch).unwrap();
        assert_eq!(later.head(), Some(missed_head));
        later.commit(person("later"), "later");
        repaired.push(&mut later).unwrap();

        let errors = repaired.take_hook_errors();
        assert_eq!(errors.len(), 1, "stale coverage is reported");
        assert!(errors[0].error.to_string().contains("coverage is stale"));

        let mut home = IndexHome::new(repaired.storage_mut(), *branch, SuccinctRollup::new());
        let manifest = home.read_manifest().unwrap();
        assert_eq!(manifest.covered, vec![indexed_head]);
        assert_eq!(manifest.segments.len(), 1, "no partial leaf was appended");
        assert!(!manifest.covers_head(later.head()));
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
