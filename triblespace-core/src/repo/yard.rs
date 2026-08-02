//! Generational collection of piles for lazy-retention blob storage.
//!
//! A [`Yard`](crate::repo::yard::Yard) keeps an ordered young-to-old sequence of [`Pile`](crate::repo::pile::Pile)
//! generations. Writes land in the youngest generation, reads search the union
//! of each generation's live PATCH set, and retention/compaction update those
//! PATCH sets without changing Pile's append-only storage contract. Call
//! [`Yard::reclaim`](crate::repo::yard::Yard::reclaim) after collection when the logically evicted blobs should
//! also be physically removed from disk.

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anybytes::Bytes;

use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use crate::id::{Id, RawId};
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding, INLINE_LEN};
use crate::patch::{Entry, IdentitySchema, PATCH};

use crate::prelude::blobencodings::SimpleArchive;

use super::pile::{
    GetBlobError, InsertError, Pile, PilePinAssertionError, PileReader, PileWriteError, ReadError,
};
use super::pin_assertion::{PinAssertion, PinAssertionSnapshot, PinAssertionStore};
use super::strong_pin::StrongPinDescriptor;
use super::want::{selected_wants_in_snapshot, WantCachePolicy, WantCachePolicySource};
use super::{
    reachable, transfer, BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut,
    PinStore, PushResult, StorageClose, TransferError,
};

type HandleSet = PATCH<INLINE_LEN, IdentitySchema>;
type StrongPins = PATCH<16, IdentitySchema, Inline<Handle<UnknownBlob>>>;

#[derive(Debug, Clone, Copy)]
pub struct YardConfig {
    /// Capacity of the canonical global asserted-want prefix used as soft
    /// cache roots. An absent selected value reserves its slot; only present
    /// members of the prefix are retained. This affects only this artifact.
    pub want_budget: usize,
    /// Strong survivor budget for the youngest level.
    pub strong_level_budget: usize,
    /// Per-level strong budget multiplier.
    pub fanout: usize,
}

impl Default for YardConfig {
    fn default() -> Self {
        Self {
            want_budget: 1024,
            strong_level_budget: 1024,
            fanout: 10,
        }
    }
}

#[derive(Debug)]
struct Segment {
    path: PathBuf,
    pile: Option<Pile>,
    live: HandleSet,
}

impl Segment {
    fn pile_mut(&mut self) -> &mut Pile {
        self.pile
            .as_mut()
            .expect("yard segment pile already closed")
    }
}

/// A generation (tier): an ordered list of segments. The youngest segment is
/// the active write target; reads union across all segments. (Today every
/// generation holds exactly one segment; multi-segment tiers land next.)
#[derive(Debug)]
struct Generation {
    segments: Vec<Segment>,
}

impl Generation {
    fn one(segment: Segment) -> Self {
        Self {
            segments: vec![segment],
        }
    }

    /// The active write segment — the youngest in the tier.
    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("yard generation has no segment")
    }

    /// Total live blobs across the tier's segments.
    fn live_len(&self) -> usize {
        self.segments.iter().map(|s| s.live.len() as usize).sum()
    }
}

/// Generational, LSM-style collection of piles.
#[derive(Debug)]
pub struct Yard {
    generations: Vec<Generation>,
    config: YardConfig,
    strong_pins: StrongPins,
}

impl Yard {
    /// Create a fresh yard, truncating/creating one pile file per generation.
    pub fn create<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        let mut generations = Vec::new();
        for path in paths {
            let path = path.as_ref().to_path_buf();
            File::create(&path).map_err(YardOpenError::Io)?;
            let pile = Pile::open(&path).map_err(|err| YardOpenError::Pile {
                path: path.clone(),
                err,
            })?;
            generations.push(Generation::one(Segment {
                path,
                pile: Some(pile),
                live: HandleSet::new(),
            }));
        }
        if generations.is_empty() {
            return Err(YardOpenError::NoGenerations);
        }
        Ok(Self {
            generations,
            config,
            strong_pins: StrongPins::new(),
        })
    }

    /// Open an existing yard and treat all blobs in each pile as live.
    ///
    /// Fails loud on corruption: a generation pile with an invalid tail
    /// surfaces as [`YardOpenError::Pile`] naming the file, and **nothing is
    /// truncated**. Repair is an explicit opt-in via [`Yard::amputate`]
    /// (mirroring [`Pile::refresh`] vs [`Pile::amputate`]).
    ///
    /// Legacy scalar-pin state is loaded exclusively from the young
    /// generation: it is the authoritative compatibility ledger, so a
    /// tombstone there can never expose a stale value from an older generation.
    pub fn open<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        Self::open_impl(paths, config, false)
    }

    /// Open an existing yard, **amputating** each generation pile first:
    /// every generation file is **TRUNCATED at its first invalid record,
    /// destroying everything after it**, exactly like [`Pile::amputate`].
    /// This is the explicit opt-in counterpart to the fail-loud [`Yard::open`] — reach for it only after `open` reported
    /// corruption and losing the invalid tail is acceptable.
    pub fn amputate<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        Self::open_impl(paths, config, true)
    }

    fn open_impl<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
        repair: bool,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        let mut generations = Vec::new();
        for path in paths {
            let path = path.as_ref().to_path_buf();
            let mut pile = Pile::open(&path).map_err(|err| YardOpenError::Pile {
                path: path.clone(),
                err,
            })?;
            let load = if repair {
                pile.amputate()
            } else {
                pile.refresh()
            };
            load.map_err(|err| YardOpenError::Pile {
                path: path.clone(),
                err,
            })?;
            let reader = pile.reader().map_err(|err| YardOpenError::Pile {
                path: path.clone(),
                err,
            })?;
            let live = collect_list(reader.blobs()).map_err(YardOpenError::List)?;
            generations.push(Generation::one(Segment {
                path,
                pile: Some(pile),
                live,
            }));
        }
        if generations.is_empty() {
            return Err(YardOpenError::NoGenerations);
        }
        // Mutable strong-pin state belongs exclusively to the young pile.
        // Older generations may retain stale pin records from an earlier
        // layout or interrupted migration; folding them together would let a
        // young tombstone resurrect an obsolete head.
        let young_path = generations[0].active_mut().path.clone();
        let durable_pins = generations[0]
            .active_mut()
            .pile_mut()
            .pin_snapshot()
            .map_err(|err| YardOpenError::Pile {
                path: young_path,
                err,
            })?;
        let mut strong_pins = StrongPins::new();
        for raw in &durable_pins {
            let head = durable_pins
                .get(raw)
                .copied()
                .expect("key from PATCH iterator must resolve in the same PATCH")
                .transmute();
            strong_pins.replace(&Entry::with_value(raw, head));
        }

        Ok(Self {
            generations,
            config,
            strong_pins,
        })
    }

    /// Number of generations in young-to-old order.
    pub fn generation_count(&self) -> usize {
        self.generations.len()
    }

    /// Number of live blobs in a generation.
    pub fn generation_len(&self, level: usize) -> Option<usize> {
        self.generations.get(level).map(|g| g.live_len())
    }

    /// Returns whether a live handle is currently associated with `level`.
    pub fn contains_in_generation<S>(&self, level: usize, handle: Inline<Handle<S>>) -> bool
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations
            .get(level)
            .is_some_and(|g| g.segments.iter().any(|s| s.live.get(&handle.raw).is_some()))
    }

    /// Strongly pin a blob as the current head for `pin`.
    pub fn pin_strong<S>(
        &mut self,
        pin: Id,
        handle: Inline<Handle<S>>,
    ) -> Result<(), PileWriteError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let new: Inline<Handle<SimpleArchive>> = handle.transmute();
        loop {
            let old = self.head(pin).expect("yard head lookup is infallible");
            match self.update(pin, old, Some(new))? {
                PushResult::Success() => return Ok(()),
                // `update` synchronizes the in-memory snapshot to the young
                // pile on conflict. Retrying makes this convenience method an
                // unconditional set even if another Pile handle appended in
                // between opening the yard and this call.
                PushResult::Conflict(_) => {}
            }
        }
    }

    /// Remove a strong pin.
    pub fn unpin_strong(&mut self, pin: Id) -> Result<(), PileWriteError> {
        loop {
            let old = self.head(pin).expect("yard head lookup is infallible");
            match self.update(pin, old, None)? {
                PushResult::Success() => return Ok(()),
                PushResult::Conflict(_) => {}
            }
        }
    }

    /// Union generic asserted-pin snapshots from every segment. Yard owns its
    /// generation files exclusively while open, so joining their grow-only
    /// sets is one coherent snapshot with no generation-order semantics.
    fn collect_pin_assertions(&mut self) -> Result<PinAssertionSnapshot, PilePinAssertionError> {
        let mut assertions = PinAssertionSnapshot::new();
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                assertions.union(segment.pile_mut().pin_assertion_snapshot()?)?;
            }
        }
        Ok(assertions)
    }

    /// Re-append the authoritative legacy scalar-pin state to the young
    /// generation after a pile rewrite. [`reclaim_generation`] transfers blobs
    /// and assertions, not mutable pin records. Missing entries are deliberately
    /// not reconstructed as tombstones: because only the young pile is
    /// authoritative, absence in the rewritten compatibility ledger is the
    /// durable deleted state and older generations are never consulted.
    fn rerecord_young_pin_state(&mut self) -> Result<(), std::io::Error> {
        let strong_pins: Vec<(Id, Inline<Handle<SimpleArchive>>)> = (&self.strong_pins)
            .into_iter()
            .map(|raw| {
                let id = Id::new(*raw).expect("nil pin id in yard strong pins");
                let head = self
                    .strong_pins
                    .get(raw)
                    .copied()
                    .expect("key from PATCH iterator must resolve in the same PATCH")
                    .transmute();
                (id, head)
            })
            .collect();
        let pile = self.generations[0].active_mut().pile_mut();
        for (id, head) in strong_pins {
            match pile.update(id, None, Some(head)).map_err(pile_write_io)? {
                PushResult::Success() => {}
                PushResult::Conflict(current) => {
                    return Err(std::io::Error::other(format!(
                        "rewritten young pile unexpectedly retained pin {id:?} at {current:?}"
                    )));
                }
            }
        }
        pile.flush().map_err(|err| match err {
            super::pile::FlushError::IoError(io) => io,
        })?;
        Ok(())
    }

    /// Recompute the keep set and logically collect soft wanted blobs and
    /// orphans.
    pub fn collect(&mut self) -> Result<(), YardCollectError> {
        let pin_assertions = self
            .collect_pin_assertions()
            .map_err(YardCollectError::PinAssertions)?;
        let reader = self.reader().map_err(YardCollectError::Reader)?;
        let durable_keep = self.durable_keep_set(&reader, &pin_assertions);
        let present = reader.live_set();
        let want_keep = asserted_want_keep_set(&pin_assertions, &present, self.want_cache_policy());

        let mut keep = durable_keep;
        keep.union(want_keep);
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                segment.live = segment.live.intersect(&keep);
            }
        }
        Ok(())
    }

    /// Run one compaction pass.
    ///
    /// Strong survivors descend when a level exceeds its strong budget.
    /// Asserted-want values are soft cache roots and remain budget-evictable at
    /// every level.
    pub fn compact(&mut self) -> Result<(), YardCollectError> {
        self.collect()?;
        let last = self.generations.len().saturating_sub(1);
        let mut dumped = Vec::new();

        {
            let pin_assertions = self
                .collect_pin_assertions()
                .map_err(YardCollectError::PinAssertions)?;
            let reader = self.reader().map_err(YardCollectError::Reader)?;
            let durable_keep = self.durable_keep_set(&reader, &pin_assertions);

            for level in 0..last {
                let durable_here = self.generations[level].segments[0]
                    .live
                    .intersect(&durable_keep);
                if durable_here.len() as usize <= self.strong_budget_for(level) {
                    continue;
                }

                // Overflow: dump the whole tier down — hard and soft
                // survivors. `collect()` above already dropped dead, so the
                // segment's `live` is exactly the survivors. Soft cache data descends to
                // use space in lower tiers rather than being pinned to the
                // youngest generation; it stays evictable everywhere and is
                // dropped by the want budget under pressure.
                let movers = self.generations[level].segments[0].live.clone();
                let handles: Vec<_> = movers
                    .clone()
                    .into_iter()
                    .map(Inline::<Handle<UnknownBlob>>::new)
                    .collect();

                let mut copied = Vec::new();
                {
                    let target = self.generations[level + 1].active_mut().pile_mut();
                    for result in transfer(&reader, target, handles.clone()) {
                        let (source, _target) = result.map_err(YardCollectError::Transfer)?;
                        copied.push(source);
                    }
                }

                {
                    let target = self.generations[level + 1].active_mut();
                    for source in copied {
                        target.live.insert(&Entry::new(&source.raw));
                    }
                }

                for raw in movers {
                    self.generations[level].segments[0].live.remove(&raw);
                }

                // Make the moved blobs durable in the target before the source
                // pile is recycled below, so a crash can't drop content that
                // would briefly live in neither place.
                self.generations[level + 1]
                    .active_mut()
                    .pile_mut()
                    .flush()
                    .map_err(YardCollectError::Flush)?;
                dumped.push(level);
            }
        }

        // Fold reclamation into the merge: each dumped tier is now empty, so
        // recycle its segment in place (crash-safe write-empty + atomic rename)
        // rather than leaving dead bytes for a separate reclaim() pass.
        for level in dumped {
            self.reclaim_segment(level, 0)
                .map_err(YardCollectError::Reclaim)?;
            // The rewrite dropped the young pile's mutable pin records along
            // with its dead bytes; re-record the complete authoritative state.
            if level == 0 {
                self.rerecord_young_pin_state()
                    .map_err(YardCollectError::PinState)?;
            }
        }

        self.collect()
    }

    /// Physically rewrite each generation's pile to contain only its live set.
    ///
    /// Collection and compaction are logical operations: they update each
    /// generation's live PATCH set, so evicted blobs stop being readable through
    /// Yard readers, but they do not mutate the underlying append-only pile
    /// files. `reclaim` is the explicit physical step. For each generation it
    /// writes the current live handles to a sibling temporary pile with
    /// [`transfer`], closes both piles, atomically renames the temporary file
    /// over the original on the same filesystem, and reopens the generation.
    pub fn reclaim(&mut self) -> Result<(), YardReclaimError> {
        for level in 0..self.generations.len() {
            for index in 0..self.generations[level].segments.len() {
                self.reclaim_segment(level, index)?;
            }
            // The rewrite dropped the young pile's mutable pin records along
            // with its dead bytes; re-record the complete authoritative state.
            if level == 0 {
                self.rerecord_young_pin_state()
                    .map_err(YardReclaimError::PinState)?;
            }
        }
        Ok(())
    }

    /// Rewrite the segment at `(level, index)` down to its live set via
    /// [`reclaim_generation`]. If the rewrite fails, reopen the generation
    /// file as-is (fail-loud: [`Pile::refresh`], no repair, no truncation)
    /// so the yard stays usable and the rewrite error propagates. If even
    /// the reopen fails — for example the file is corrupt — both errors
    /// propagate together via [`YardReclaimError::Reopen`] and the segment
    /// is left closed.
    fn reclaim_segment(&mut self, level: usize, index: usize) -> Result<(), YardReclaimError> {
        let segment = &mut self.generations[level].segments[index];
        let path = segment.path.clone();
        let temp_path = reclaim_temp_path(&path, level);
        let live = segment.live.clone();
        let pile = segment
            .pile
            .take()
            .expect("yard segment pile already closed");

        match reclaim_generation(&path, &temp_path, &live, pile) {
            Ok(pile) => {
                self.generations[level].segments[index].pile = Some(pile);
                Ok(())
            }
            Err(primary) => {
                let reopen = Pile::open(&path).and_then(|mut pile| {
                    pile.refresh()?;
                    Ok(pile)
                });
                match reopen {
                    Ok(pile) => {
                        self.generations[level].segments[index].pile = Some(pile);
                        Err(primary)
                    }
                    Err(err) => Err(YardReclaimError::Reopen {
                        path,
                        primary: Box::new(primary),
                        err,
                    }),
                }
            }
        }
    }

    fn strong_budget_for(&self, level: usize) -> usize {
        let multiplier = self.config.fanout.max(1).saturating_pow(level as u32);
        self.config.strong_level_budget.saturating_mul(multiplier)
    }

    fn strong_keep_set(&self, reader: &YardReader) -> HandleSet {
        let roots: Vec<_> = (&self.strong_pins)
            .into_iter()
            .filter_map(|pin| self.strong_pins.get(pin).copied())
            .collect();

        let mut keep = HandleSet::new();
        for handle in reachable(reader, roots) {
            keep.insert(&Entry::new(&handle.raw));
        }
        keep
    }

    /// Keep set for state that may never be silently evicted.
    ///
    /// Legacy scalar pins remain hard compatibility roots. Assertions whose
    /// locally present outer descriptor is a canonical strong wrapper retain
    /// the wrapped descriptor and every asserted value's local closure. Wants
    /// are handled separately as budgeted soft roots and never veto a hard
    /// edge.
    fn durable_keep_set(
        &self,
        reader: &YardReader,
        pin_assertions: &PinAssertionSnapshot,
    ) -> HandleSet {
        let mut keep = self.strong_keep_set(reader);
        keep.union(strong_pin_keep_set(reader, pin_assertions));
        keep
    }

    #[cfg(test)]
    fn put_in_generation<S, T>(
        &mut self,
        level: usize,
        item: T,
    ) -> Result<Inline<Handle<S>>, InsertError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let handle = self.generations[level]
            .active_mut()
            .pile_mut()
            .put::<S, T>(item)?;
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations[level]
            .active_mut()
            .live
            .insert(&Entry::new(&unknown.raw));
        Ok(handle)
    }
}

impl PinAssertionStore for Yard {
    type Error = PilePinAssertionError;

    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
        self.collect_pin_assertions()
    }

    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
        // Avoid copying an assertion into young when any generation already
        // carries the exact witness. A collision is surfaced before append.
        if self.collect_pin_assertions()?.contains(&assertion)? {
            return Ok(());
        }
        self.generations[0]
            .active_mut()
            .pile_mut()
            .append_pin_assertion(assertion)
    }
}

impl WantCachePolicySource for Yard {
    fn want_cache_policy(&self) -> WantCachePolicy {
        WantCachePolicy::bounded(self.config.want_budget)
    }
}

impl PinStore for Yard {
    type PinsError = Infallible;
    type HeadError = Infallible;
    type UpdateError = PileWriteError;

    type ListIter<'a> = std::vec::IntoIter<Result<Id, Infallible>>;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        // Byte-ordered (PATCH tree order) for deterministic iteration,
        // mirroring Pile's PATCH-backed `pins`.
        let ids: Vec<Result<Id, Infallible>> = self
            .strong_pins
            .clone()
            .into_iter_ordered()
            .map(|raw| Ok(Id::new(raw).expect("nil pin id in yard strong pins")))
            .collect();
        Ok(ids.into_iter())
    }

    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        let raw: RawId = id.into();
        Ok(self.strong_pins.get(&raw).copied().map(Inline::transmute))
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let raw: RawId = id.into();
        let current: Option<Inline<Handle<SimpleArchive>>> =
            self.strong_pins.get(&raw).copied().map(Inline::transmute);
        if current != old {
            return Ok(PushResult::Conflict(current));
        }
        if current == new {
            return Ok(PushResult::Success());
        }

        // The young Pile is the durable source of truth. Append and replay
        // its record first; only publish the new in-memory snapshot after the
        // write succeeds. A failed write therefore cannot expose state that a
        // reopen would forget.
        let persisted = self.generations[0]
            .active_mut()
            .pile_mut()
            .update(id, old, new)?;
        let (persisted_head, outcome) = match persisted {
            PushResult::Success() => (new, PushResult::Success()),
            PushResult::Conflict(actual) => (actual, PushResult::Conflict(actual)),
        };
        match persisted_head {
            Some(head) => {
                self.strong_pins
                    .replace(&Entry::with_value(&raw, head.transmute()));
            }
            None => self.strong_pins.remove(&raw),
        }
        Ok(outcome)
    }
}

impl Drop for Yard {
    fn drop(&mut self) {
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                if let Some(pile) = segment.pile.take() {
                    let _ = pile.close();
                }
            }
        }
    }
}

impl BlobStorePut for Yard {
    type PutError = InsertError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let handle = self.generations[0]
            .active_mut()
            .pile_mut()
            .put::<S, T>(item)?;
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations[0]
            .active_mut()
            .live
            .insert(&Entry::new(&unknown.raw));
        Ok(handle)
    }
}

impl BlobStore for Yard {
    type Reader = YardReader;
    type ReaderError = YardReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let mut generations = Vec::new();
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                generations.push(YardGenerationReader {
                    reader: segment.pile_mut().reader().map_err(YardReaderError::Pile)?,
                    live: segment.live.clone(),
                });
            }
        }
        Ok(YardReader { generations })
    }
}

impl super::StorageFlush for Yard {
    type Error = super::pile::FlushError;

    /// Flush every open generation pile. Fresh writes land in the young
    /// generation, but older generations can hold unsynced rewrites from
    /// `reclaim`/`compact`, so sync them all.
    fn flush(&mut self) -> Result<(), Self::Error> {
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                if let Some(pile) = segment.pile.as_mut() {
                    pile.flush()?;
                }
            }
        }
        Ok(())
    }
}

impl StorageClose for Yard {
    type Error = YardCloseError;

    fn close(mut self) -> Result<(), Self::Error> {
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                if let Some(pile) = segment.pile.take() {
                    pile.close().map_err(YardCloseError::Pile)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct YardGenerationReader {
    reader: PileReader,
    live: HandleSet,
}

impl PartialEq for YardGenerationReader {
    fn eq(&self, other: &Self) -> bool {
        self.reader == other.reader && self.live == other.live
    }
}

impl Eq for YardGenerationReader {}

/// Read-only Yard snapshot.
#[derive(Debug, Clone)]
pub struct YardReader {
    generations: Vec<YardGenerationReader>,
}

impl YardReader {
    fn live_set(&self) -> HandleSet {
        let mut live = HandleSet::new();
        for generation in &self.generations {
            live.union(generation.live.clone());
        }
        live
    }

    /// Union read across generations (young -> old), returning `None` on a
    /// clean miss. Raw storage reads are observational: the explicit lazy
    /// layer owns the signing key and records asserted wants when appropriate.
    fn get_local<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Option<Result<T, YardGetError<<T as TryFromBlob<S>>::Error>>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        for generation in &self.generations {
            if generation.live.get(&unknown.raw).is_none() {
                continue;
            }
            match generation.reader.get::<T, S>(handle) {
                Ok(value) => return Some(Ok(value)),
                Err(GetBlobError::BlobNotFound) => continue,
                Err(err) => return Some(Err(YardGetError::Pile(err))),
            }
        }
        None
    }

    /// Discover locally present children without demand side effects.
    fn local_children(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Vec<Inline<Handle<UnknownBlob>>> {
        let Some(Ok(blob)) = self.get_local::<Blob<UnknownBlob>, UnknownBlob>(handle) else {
            return Vec::new();
        };
        let bytes = blob.bytes.as_ref();
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + INLINE_LEN <= bytes.len() {
            let mut raw = [0u8; INLINE_LEN];
            raw.copy_from_slice(&bytes[offset..offset + INLINE_LEN]);
            let candidate = Inline::<Handle<UnknownBlob>>::new(raw);
            if matches!(self.get_local::<Bytes, UnknownBlob>(candidate), Some(Ok(_))) {
                result.push(candidate);
            }
            offset += INLINE_LEN;
        }
        result
    }
}

impl PartialEq for YardReader {
    fn eq(&self, other: &Self) -> bool {
        self.generations == other.generations
    }
}

impl Eq for YardReader {}

impl BlobStoreGet for YardReader {
    type GetError<E: Error + Send + Sync + 'static> = YardGetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.get_local::<T, S>(handle)
            .unwrap_or(Err(YardGetError::NotFound))
    }
}

impl BlobChildren for YardReader {
    fn children(&self, handle: Inline<Handle<UnknownBlob>>) -> Vec<Inline<Handle<UnknownBlob>>> {
        self.local_children(handle)
    }
}

impl super::branch_frontier::PartialCommitDag for YardReader {
    type Error = super::commit::StoredCommitError<
        YardGetError<crate::blob::encodings::simplearchive::UnarchiveError>,
    >;

    fn parents(
        &mut self,
        commit: super::CommitHandle,
    ) -> Result<super::branch_frontier::ParentLookup, Self::Error> {
        use super::branch_frontier::ParentLookup;

        // Branch resolution performs optimistic ancestry probes before the
        // corresponding assertion necessarily reaches authentication. Raw
        // storage reads are observational, so a missing ancestor remains a
        // local `Missing` result rather than creating demand implicitly.
        match self.get_local::<crate::trible::TribleSet, SimpleArchive>(commit) {
            Some(Ok(metadata)) => super::commit::direct_parents(&metadata)
                .map(ParentLookup::Present)
                .map_err(super::commit::StoredCommitError::Metadata),
            None => Ok(ParentLookup::Missing),
            Some(Err(err)) => Err(super::commit::StoredCommitError::Read(err)),
        }
    }
}

impl BlobStoreList for YardReader {
    type Iter<'a> = YardListIter;
    type Err = Infallible;

    fn blobs(&self) -> Self::Iter<'_> {
        YardListIter {
            inner: self.live_set().into_iter(),
        }
    }
}

pub struct YardListIter {
    inner: crate::patch::PATCHIntoIterator<INLINE_LEN, IdentitySchema, ()>,
}

impl Iterator for YardListIter {
    type Item = Result<Inline<Handle<UnknownBlob>>, Infallible>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(Inline::<Handle<UnknownBlob>>::new)
            .map(Ok)
    }
}

fn pile_write_io(err: PileWriteError) -> std::io::Error {
    match err {
        PileWriteError::IoError(io) => io,
    }
}

fn collect_list<E>(
    iter: impl IntoIterator<Item = Result<Inline<Handle<UnknownBlob>>, E>>,
) -> Result<HandleSet, E> {
    let mut set = HandleSet::new();
    for result in iter {
        let handle = result?;
        set.insert(&Entry::new(&handle.raw));
    }
    Ok(set)
}

/// Retain the locally present members of the canonical asserted-want prefix.
///
/// Assertions are durable intent; retention is artifact-local cache policy.
/// Evicting a wanted blob therefore never erases its assertion, and a later
/// reconciliation may fetch it again. Selection happens before the presence
/// test, so an absent low-ranked value reserves its slot and collection agrees
/// exactly with the reconciler's fetch policy.
fn asserted_want_keep_set(
    assertions: &PinAssertionSnapshot,
    present: &HandleSet,
    policy: WantCachePolicy,
) -> HandleSet {
    let mut keep = HandleSet::new();
    for handle in selected_wants_in_snapshot(assertions, policy) {
        if present.get(&handle.raw).is_none() {
            continue;
        }
        keep.insert(&Entry::new(&handle.raw));
    }
    keep
}

/// Project generic strong-retention wrappers out of the asserted-pin set.
///
/// The wrapper is the complete generic policy boundary. Yard neither knows nor
/// guesses the inner kind: once a locally present outer descriptor decodes
/// exactly, the outer itself is retained and the wrapped descriptor plus every
/// distinct authentic assertion value becomes a conservative local-closure
/// root. Missing or malformed outers are neutral while their assertion records
/// remain durable. Publication flushes dependency blobs before the assertion,
/// while replication may safely deliver them in either order.
fn strong_pin_keep_set(reader: &YardReader, assertions: &PinAssertionSnapshot) -> HandleSet {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    let mut values_by_pin = BTreeMap::<_, BTreeSet<_>>::new();
    for assertion in assertions.iter() {
        values_by_pin
            .entry(assertion.identity().pin())
            .or_default()
            .insert(assertion.value());
    }

    let mut queue = VecDeque::new();
    let mut keep = HandleSet::new();

    for (pin, values) in values_by_pin {
        let outer = StrongPinDescriptor::descriptor_handle(pin);
        let Some(Ok(inner)) =
            reader.get_local::<Inline<Handle<UnknownBlob>>, StrongPinDescriptor>(outer)
        else {
            continue;
        };

        keep.insert(&Entry::new(&outer.raw));
        queue.push_back(inner);
        queue.extend(
            values
                .into_iter()
                .map(|value| Inline::<Handle<UnknownBlob>>::new(value.raw())),
        );
    }

    while let Some(handle) = queue.pop_front() {
        if keep.get(&handle.raw).is_some() {
            continue;
        }
        keep.insert(&Entry::new(&handle.raw));
        for child in reader.local_children(handle) {
            if keep.get(&child.raw).is_none() {
                queue.push_back(child);
            }
        }
    }
    keep
}

fn reclaim_generation(
    path: &Path,
    temp_path: &Path,
    live: &HandleSet,
    mut old_pile: Pile,
) -> Result<Pile, YardReclaimError> {
    match fs::remove_file(temp_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(YardReclaimError::Io(err)),
    }

    // Assertions are immutable set members. Copy them before the atomic
    // rename: re-appending after replacement would leave a crash window in
    // which accepted replicated state had vanished. Witnesses are reproduced
    // structurally, including invalid signatures retained for diagnostics.
    let pin_assertions = old_pile
        .pin_assertion_snapshot()
        .map_err(YardReclaimError::PinAssertions)?;
    let reader = old_pile.reader().map_err(YardReclaimError::Pile)?;
    File::create(temp_path).map_err(YardReclaimError::Io)?;
    let mut new_pile = Pile::open(temp_path).map_err(YardReclaimError::Pile)?;
    let handles: Vec<_> = live
        .clone()
        .into_iter()
        .map(Inline::<Handle<UnknownBlob>>::new)
        .collect();

    for result in transfer(&reader, &mut new_pile, handles) {
        result.map_err(YardReclaimError::Transfer)?;
    }

    for assertion in pin_assertions.iter_unverified() {
        new_pile
            .append_replayed_pin_assertion(assertion)
            .map_err(YardReclaimError::PinAssertions)?;
    }

    new_pile.close().map_err(YardReclaimError::Close)?;
    drop(reader);
    old_pile.close().map_err(YardReclaimError::Close)?;
    fs::rename(temp_path, path).map_err(YardReclaimError::Io)?;

    let mut reopened = Pile::open(path).map_err(YardReclaimError::Pile)?;
    // The rewritten pile was just written and closed by us; fail loud on
    // any validation error rather than repair-truncating it.
    reopened.refresh().map_err(YardReclaimError::Pile)?;
    Ok(reopened)
}

fn reclaim_temp_path(path: &Path, level: usize) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "generation".into());
    path.with_file_name(format!(
        ".{file_name}.reclaim-{}-{level}.tmp",
        std::process::id()
    ))
}

#[derive(Debug)]
pub enum YardOpenError {
    NoGenerations,
    Io(std::io::Error),
    /// A generation pile failed to open or validate. A
    /// [`ReadError::CorruptPile`] here means the named generation file has
    /// an invalid tail; nothing was truncated — repair explicitly with
    /// [`Yard::amputate`] if losing the tail is acceptable.
    Pile {
        /// The generation pile file that failed.
        path: PathBuf,
        /// The underlying pile error.
        err: ReadError,
    },
    List(GetBlobError<Infallible>),
}

impl fmt::Display for YardOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoGenerations => write!(f, "yard requires at least one generation"),
            Self::Io(err) => write!(f, "failed to create yard pile file: {err}"),
            Self::Pile { path, err } => {
                write!(
                    f,
                    "failed to open yard generation pile {}: {err}",
                    path.display()
                )
            }
            Self::List(err) => write!(f, "failed to list yard pile: {err}"),
        }
    }
}

impl Error for YardOpenError {}

#[derive(Debug)]
pub enum YardReaderError {
    Pile(ReadError),
}

impl fmt::Display for YardReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pile(err) => write!(f, "failed to read yard generation: {err}"),
        }
    }
}

impl Error for YardReaderError {}

#[derive(Debug)]
pub enum YardGetError<E: Error> {
    NotFound,
    Pile(GetBlobError<E>),
}

impl<E: Error> fmt::Display for YardGetError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "blob not found in yard"),
            Self::Pile(err) => write!(f, "yard generation read failed: {err}"),
        }
    }
}

impl<E: Error + 'static> Error for YardGetError<E> {}

#[derive(Debug)]
pub enum YardCollectError {
    PinAssertions(PilePinAssertionError),
    Reader(YardReaderError),
    Transfer(TransferError<Infallible, YardGetError<Infallible>, InsertError>),
    Flush(super::pile::FlushError),
    Reclaim(YardReclaimError),
    PinState(std::io::Error),
}

impl fmt::Display for YardCollectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PinAssertions(err) => {
                write!(f, "failed to snapshot generic yard pin assertions: {err}")
            }
            Self::Reader(err) => write!(f, "failed to create yard reader: {err}"),
            Self::Transfer(err) => write!(f, "failed to compact yard generation: {err}"),
            Self::Flush(err) => write!(f, "failed to flush yard generation pile: {err}"),
            Self::Reclaim(err) => {
                write!(f, "failed to recycle compacted yard generation: {err}")
            }
            Self::PinState(err) => {
                write!(f, "failed to re-record young-generation pin state: {err}")
            }
        }
    }
}

impl Error for YardCollectError {}

#[derive(Debug)]
pub enum YardCloseError {
    Pile(super::pile::FlushError),
}

impl fmt::Display for YardCloseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pile(err) => write!(f, "failed to close yard pile: {err}"),
        }
    }
}

impl Error for YardCloseError {}

#[derive(Debug)]
pub enum YardReclaimError {
    Io(std::io::Error),
    Pile(ReadError),
    PinAssertions(PilePinAssertionError),
    Transfer(TransferError<Infallible, GetBlobError<Infallible>, InsertError>),
    Close(super::pile::FlushError),
    PinState(std::io::Error),
    /// A generation rewrite failed (`primary`) and the subsequent
    /// fail-loud reopen of the generation file also failed (`err`). The
    /// segment is left closed; nothing was truncated.
    Reopen {
        /// The generation pile file that could not be reopened.
        path: PathBuf,
        /// The rewrite error that triggered the reopen.
        primary: Box<YardReclaimError>,
        /// The reopen/validation error.
        err: ReadError,
    },
}

impl fmt::Display for YardReclaimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "failed to replace yard generation pile: {err}"),
            Self::Pile(err) => write!(f, "failed to read yard generation pile: {err}"),
            Self::PinAssertions(err) => {
                write!(f, "failed to preserve generic yard pin assertions: {err}")
            }
            Self::Transfer(err) => write!(f, "failed to copy live yard blobs: {err}"),
            Self::Close(err) => write!(f, "failed to close yard generation pile: {err}"),
            Self::PinState(err) => {
                write!(f, "failed to re-record young-generation pin state: {err}")
            }
            Self::Reopen { path, primary, err } => write!(
                f,
                "failed to reopen yard generation pile {} after failed rewrite ({primary}): {err}",
                path.display()
            ),
        }
    }
}

impl Error for YardReclaimError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::encodings::longstring::LongString;
    use crate::blob::encodings::rawbytes::RawBytes;
    use crate::repo::branch_pin::BranchPinDescriptor;
    use crate::repo::commit;
    use crate::repo::pin_assertion::{
        PinHandle, SubsumptionLabel, UnverifiedPinAssertion, ValueHandle,
    };
    use crate::repo::strong_pin::StrongPinDescriptor;
    use crate::repo::want::{all_wants_in_snapshot, sign_want, wants_in_snapshot};
    use crate::repo::StorageFlush;
    use ed25519_dalek::SigningKey;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    fn yard_with_paths(
        generations: usize,
        config: YardConfig,
    ) -> (tempfile::TempDir, Vec<PathBuf>, Yard) {
        let dir = tempfile::tempdir().unwrap();
        let paths = (0..generations)
            .map(|i| dir.path().join(format!("gen-{i}.pile")))
            .collect::<Vec<_>>();
        let yard = Yard::create(paths.clone(), config).unwrap();
        (dir, paths, yard)
    }

    fn yard_with(generations: usize, config: YardConfig) -> (tempfile::TempDir, Yard) {
        let (dir, _paths, yard) = yard_with_paths(generations, config);
        (dir, yard)
    }

    fn raw_blob(bytes: &'static [u8]) -> Bytes {
        Bytes::from_source(bytes.to_vec())
    }

    fn pin_id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn pin_assertion(value: u8) -> PinAssertion {
        PinAssertion::sign(
            &SigningKey::from_bytes(&[7; 32]),
            PinHandle::from_raw([11; 32]),
            ValueHandle::from_raw([value; 32]),
            SubsumptionLabel::from_raw([value; 32]),
        )
    }

    fn append_want<S>(yard: &mut Yard, key: &SigningKey, handle: Inline<Handle<S>>)
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        yard.append_pin_assertion(sign_want(key, handle)).unwrap();
    }

    fn append_strong(
        yard: &mut Yard,
        key: &SigningKey,
        inner: Inline<Handle<UnknownBlob>>,
        value: Inline<Handle<UnknownBlob>>,
        label: u8,
    ) -> PinAssertion {
        let assertion = PinAssertion::sign(
            key,
            StrongPinDescriptor::pin_handle(inner),
            ValueHandle::from_raw(value.raw),
            SubsumptionLabel::from_raw([label; 32]),
        );
        yard.append_pin_assertion(assertion).unwrap();
        assertion
    }

    fn get_raw(
        reader: &YardReader,
        handle: Inline<Handle<RawBytes>>,
    ) -> Result<Bytes, YardGetError<Infallible>> {
        reader.get::<Bytes, RawBytes>(handle)
    }

    fn pile_blob_count(path: &Path) -> usize {
        let mut pile = Pile::open(path).unwrap();
        pile.refresh().unwrap();
        let reader = pile.reader().unwrap();
        let count = reader.blobs().collect::<Result<Vec<_>, _>>().unwrap().len();
        drop(reader);
        pile.close().unwrap();
        count
    }

    #[test]
    fn generation_union_read_finds_older_generation() {
        let (_dir, mut yard) = yard_with(2, YardConfig::default());
        let old = yard
            .put_in_generation::<RawBytes, _>(1, raw_blob(b"old generation"))
            .unwrap();

        let reader = yard.reader().unwrap();

        assert_eq!(get_raw(&reader, old).unwrap(), raw_blob(b"old generation"));
    }

    #[test]
    fn pin_assertions_union_across_generations_and_survive_reclaim() {
        let (_dir, paths, mut yard) = yard_with_paths(2, YardConfig::default());
        let young = pin_assertion(19);
        let old = pin_assertion(23);

        yard.append_pin_assertion(young).unwrap();
        yard.generations[1]
            .active_mut()
            .pile_mut()
            .append_pin_assertion(old)
            .unwrap();

        let young_len = std::fs::metadata(&paths[0]).unwrap().len();
        yard.append_pin_assertion(old).unwrap();
        assert_eq!(
            std::fs::metadata(&paths[0]).unwrap().len(),
            young_len,
            "an assertion already present in old must not be copied into young"
        );

        let before = yard.pin_assertion_snapshot().unwrap();
        assert_eq!(before.len(), 2);
        yard.reclaim().unwrap();
        assert_eq!(yard.pin_assertion_snapshot().unwrap(), before);
        yard.close().unwrap();

        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        assert_eq!(reopened.pin_assertion_snapshot().unwrap(), before);
        reopened.close().unwrap();
    }

    #[test]
    fn reclaim_preserves_structural_pin_witnesses_without_promoting_them() {
        let (_dir, paths, mut yard) = yard_with_paths(1, YardConfig::default());
        let child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"forged child".to_vec()))
            .unwrap();
        let mut inner_bytes = vec![0u8; 64];
        inner_bytes[32..].copy_from_slice(&child.raw);
        let inner = yard
            .put::<UnknownBlob, _>(Bytes::from_source(inner_bytes))
            .unwrap();
        let mut value_bytes = vec![1u8; 64];
        value_bytes[32..].copy_from_slice(&child.raw);
        let value = yard
            .put::<UnknownBlob, _>(Bytes::from_source(value_bytes))
            .unwrap();
        let outer = yard
            .put::<StrongPinDescriptor, _>(StrongPinDescriptor::blob(inner))
            .unwrap();
        let mut bytes = PinAssertion::sign(
            &SigningKey::from_bytes(&[19; 32]),
            PinHandle::from_raw(outer.raw),
            ValueHandle::from_raw(value.raw),
            SubsumptionLabel::from_raw([19; 32]),
        )
        .encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let invalid = UnverifiedPinAssertion::decode_structural(bytes).unwrap();
        yard.generations[0]
            .active_mut()
            .pile_mut()
            .append_replayed_pin_assertion(invalid)
            .unwrap();

        let before = yard.pin_assertion_snapshot().unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before.iter().count(), 0);
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();
        for handle in [outer.transmute(), inner, value, child] {
            assert!(matches!(
                reader.get::<Blob<UnknownBlob>, UnknownBlob>(handle),
                Err(YardGetError::NotFound)
            ));
        }
        drop(reader);
        yard.reclaim().unwrap();
        assert_eq!(yard.pin_assertion_snapshot().unwrap(), before);
        yard.close().unwrap();

        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        let after = reopened.pin_assertion_snapshot().unwrap();
        assert_eq!(after, before);
        assert_eq!(after.len(), 1);
        assert_eq!(after.iter().count(), 0);
        reopened.close().unwrap();
    }

    #[test]
    fn optimistic_branch_probes_are_observational() {
        use crate::repo::branch_frontier::{resolve_branch, BranchResolution};
        use crate::repo::branch_pin::{sign_branch_assertion, BranchIdentity, BranchRank};

        let (_dir, mut yard) = yard_with(1, YardConfig::default());
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let name = Inline::<Handle<LongString>>::new([3; 32]);
        let identity = BranchIdentity::new(signing_key.verifying_key(), name);
        let resident = yard
            .put(commit::commit_metadata(
                &signing_key,
                [],
                None,
                Some(crate::trible::TribleSet::new().to_blob()),
                None,
            ))
            .unwrap();
        let forged_missing = Inline::<Handle<SimpleArchive>>::new([0xE1; 32]);

        let mut snapshot = PinAssertionSnapshot::new();
        snapshot
            .insert(sign_branch_assertion(
                &signing_key,
                name,
                resident,
                BranchRank::ROOT,
            ))
            .unwrap();
        let mut forged = sign_branch_assertion(
            &signing_key,
            name,
            forged_missing,
            BranchRank::ROOT.successor().unwrap(),
        )
        .encode();
        let last = forged.len() - 1;
        forged[last] ^= 1;
        snapshot
            .insert_unverified(UnverifiedPinAssertion::decode_structural(forged).unwrap())
            .unwrap();

        let before = yard.pin_assertion_snapshot().unwrap();
        let mut reader = yard.reader().unwrap();
        let BranchResolution::Complete(frontier) =
            resolve_branch(&snapshot, &identity, &mut reader).unwrap()
        else {
            panic!("discarding the forged claim must reveal the resident singleton")
        };
        assert_eq!(frontier.tips(), &[resident]);
        drop(reader);
        assert_eq!(
            yard.pin_assertion_snapshot().unwrap(),
            before,
            "raw ancestry probes must not create asserted demand"
        );
    }

    #[test]
    fn unknown_generic_pin_kinds_are_preserved_but_retention_neutral() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let value = yard
            .put::<RawBytes, _>(raw_blob(b"unknown pin value"))
            .unwrap();
        let assertion = PinAssertion::sign(
            &SigningKey::from_bytes(&[7; 32]),
            PinHandle::from_raw([0xA5; 32]),
            ValueHandle::from_raw(value.raw),
            SubsumptionLabel::from_raw([1; 32]),
        );
        yard.append_pin_assertion(assertion).unwrap();

        yard.collect().unwrap();
        assert!(matches!(
            get_raw(&yard.reader().unwrap(), value),
            Err(YardGetError::NotFound)
        ));
        assert_eq!(
            yard.pin_assertion_snapshot()
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![assertion],
            "opaque replicated state survives independently of blob retention policy"
        );
    }

    #[test]
    fn strong_descriptor_retains_unknown_inner_and_all_authentic_value_closures() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let first_child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"first child".to_vec()))
            .unwrap();
        let second_child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"second child".to_vec()))
            .unwrap();

        let mut inner_bytes = vec![0x31; 64];
        inner_bytes[32..].copy_from_slice(&first_child.raw);
        let inner = yard
            .put::<UnknownBlob, _>(Bytes::from_source(inner_bytes))
            .unwrap();
        let outer = yard
            .put::<StrongPinDescriptor, _>(StrongPinDescriptor::blob(inner))
            .unwrap();

        let mut first_value_bytes = vec![0x41; 64];
        first_value_bytes[32..].copy_from_slice(&first_child.raw);
        let first_value = yard
            .put::<UnknownBlob, _>(Bytes::from_source(first_value_bytes))
            .unwrap();
        let mut second_value_bytes = vec![0x51; 64];
        second_value_bytes[32..].copy_from_slice(&second_child.raw);
        let second_value = yard
            .put::<UnknownBlob, _>(Bytes::from_source(second_value_bytes))
            .unwrap();

        append_strong(
            &mut yard,
            &SigningKey::from_bytes(&[7; 32]),
            inner,
            first_value,
            1,
        );
        append_strong(
            &mut yard,
            &SigningKey::from_bytes(&[8; 32]),
            inner,
            second_value,
            2,
        );
        let dead = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"unrooted".to_vec()))
            .unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();
        let decoded_inner: Inline<Handle<UnknownBlob>> = reader.get(outer).unwrap();
        assert_eq!(decoded_inner, inner);
        for handle in [inner, first_value, second_value, first_child, second_child] {
            assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(handle).is_ok());
        }
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(dead),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn missing_or_malformed_strong_outer_is_neutral_until_exact_outer_arrives() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let key = SigningKey::from_bytes(&[9; 32]);

        let child_blob = Blob::<UnknownBlob>::new(Bytes::from_source(b"late child".to_vec()));
        let child = child_blob.get_handle();
        let mut inner_bytes = vec![0x61; 64];
        inner_bytes[32..].copy_from_slice(&child.raw);
        let inner_blob = Blob::<UnknownBlob>::new(Bytes::from_source(inner_bytes));
        let inner = inner_blob.get_handle();
        let outer_blob = StrongPinDescriptor::blob(inner);
        let outer = outer_blob.get_handle();
        let mut value_bytes = vec![0x71; 64];
        value_bytes[32..].copy_from_slice(&child.raw);
        let value_blob = Blob::<UnknownBlob>::new(Bytes::from_source(value_bytes));
        let value = value_blob.get_handle();

        // Assertion first: the value is locally present, but an absent outer
        // descriptor cannot confer retention semantics.
        append_strong(&mut yard, &key, inner, value, 3);
        yard.put::<UnknownBlob, _>(value_blob.clone()).unwrap();
        yard.put::<UnknownBlob, _>(child_blob.clone()).unwrap();

        let mut malformed_bytes = StrongPinDescriptor::encode(inner);
        malformed_bytes[0] ^= 1;
        let malformed_blob =
            Blob::<StrongPinDescriptor>::new(Bytes::from_source(malformed_bytes.to_vec()));
        let malformed = malformed_blob.get_handle();
        let malformed_value_blob =
            Blob::<UnknownBlob>::new(Bytes::from_source(b"malformed value".to_vec()));
        let malformed_value = malformed_value_blob.get_handle();
        yard.put::<StrongPinDescriptor, _>(malformed_blob.clone())
            .unwrap();
        yard.put::<UnknownBlob, _>(malformed_value_blob.clone())
            .unwrap();
        let malformed_assertion = PinAssertion::sign(
            &key,
            PinHandle::from_raw(malformed.raw),
            ValueHandle::from_raw(malformed_value.raw),
            SubsumptionLabel::from_raw([4; 32]),
        );
        yard.append_pin_assertion(malformed_assertion).unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();
        for handle in [value, child, malformed_value] {
            assert!(matches!(
                reader.get::<Blob<UnknownBlob>, UnknownBlob>(handle),
                Err(YardGetError::NotFound)
            ));
        }
        assert!(matches!(
            reader.get::<Blob<StrongPinDescriptor>, StrongPinDescriptor>(malformed),
            Err(YardGetError::NotFound)
        ));
        drop(reader);

        // Exact descriptor arrival activates the already durable assertion.
        yard.put::<UnknownBlob, _>(child_blob).unwrap();
        yard.put::<UnknownBlob, _>(inner_blob).unwrap();
        yard.put::<UnknownBlob, _>(value_blob).unwrap();
        assert_eq!(
            yard.put::<StrongPinDescriptor, _>(outer_blob).unwrap(),
            outer
        );
        // Reintroducing malformed content still grants no root.
        yard.put::<StrongPinDescriptor, _>(malformed_blob).unwrap();
        yard.put::<UnknownBlob, _>(malformed_value_blob).unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();
        let decoded: Inline<Handle<UnknownBlob>> = reader.get(outer).unwrap();
        assert_eq!(decoded, inner);
        for handle in [inner, value, child] {
            assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(handle).is_ok());
        }
        assert!(matches!(
            reader.get::<Blob<StrongPinDescriptor>, StrongPinDescriptor>(malformed),
            Err(YardGetError::NotFound)
        ));
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(malformed_value),
            Err(YardGetError::NotFound)
        ));
        assert_eq!(yard.pin_assertion_snapshot().unwrap().iter().count(), 2);
    }

    #[test]
    fn hard_roots_survive_when_asserted_wants_have_zero_budget() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let strong = yard.put::<RawBytes, _>(raw_blob(b"strong")).unwrap();
        let key = SigningKey::from_bytes(&[8; 32]);
        let wanted = Blob::<RawBytes>::new(raw_blob(b"wanted")).get_handle();
        append_want(&mut yard, &key, wanted);
        yard.put::<RawBytes, _>(raw_blob(b"wanted")).unwrap();

        yard.pin_strong(pin_id(1), strong).unwrap();
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert_eq!(get_raw(&reader, strong).unwrap(), raw_blob(b"strong"));
        assert!(matches!(
            get_raw(&reader, wanted),
            Err(YardGetError::NotFound)
        ));
        drop(reader);
        assert_eq!(
            wants_in_snapshot(&yard.pin_assertion_snapshot().unwrap(), key.verifying_key()),
            BTreeSet::from([wanted.transmute()]),
            "cache eviction must not erase durable intent"
        );
    }

    #[test]
    fn absent_prefix_want_reserves_capacity_without_displacing_hard_roots() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 2,
                ..YardConfig::default()
            },
        );
        let key = SigningKey::from_bytes(&[18; 32]);
        let mut candidates = [
            b"policy-candidate-a".as_slice(),
            b"policy-candidate-b".as_slice(),
            b"policy-candidate-c".as_slice(),
            b"policy-candidate-d".as_slice(),
        ]
        .into_iter()
        .map(|source| {
            let bytes = Bytes::from_source(source.to_vec());
            let handle = Blob::<RawBytes>::new(bytes.clone()).get_handle();
            (handle, bytes)
        })
        .collect::<Vec<_>>();
        candidates.sort_by_key(|(handle, _)| handle.raw);

        let absent = candidates[0].0;
        let selected_present = candidates[1].0;
        let unselected_present = candidates[2].0;
        let hard_outside_prefix = candidates[3].0;
        for (handle, bytes) in &candidates[1..] {
            let stored = yard.put::<RawBytes, _>(bytes.clone()).unwrap();
            assert_eq!(stored, *handle);
        }
        // Insert in reverse rank order to make ordering visibly independent
        // from record order.
        for (handle, _) in candidates.iter().rev() {
            append_want(&mut yard, &key, *handle);
        }
        yard.pin_strong(pin_id(18), hard_outside_prefix).unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(matches!(
            get_raw(&reader, absent),
            Err(YardGetError::NotFound)
        ));
        assert!(get_raw(&reader, selected_present).is_ok());
        assert!(matches!(
            get_raw(&reader, unselected_present),
            Err(YardGetError::NotFound)
        ));
        assert!(
            get_raw(&reader, hard_outside_prefix).is_ok(),
            "hard roots survive independently of asserted-want selection"
        );
    }

    #[test]
    fn branch_pin_closure_remains_hard_when_its_values_are_also_soft_wants() {
        use crate::blob::encodings::longstring::LongString;
        use crate::repo::branch_pin::{sign_branch_assertion, BranchRank};
        use crate::repo::commit;
        use crate::trible::TribleSet;
        use ed25519_dalek::SigningKey;

        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let key = SigningKey::from_bytes(&[7; 32]);

        let content_blob: Blob<SimpleArchive> = TribleSet::new().to_blob();
        let content = content_blob.get_handle();
        let parent_blob: Blob<SimpleArchive> =
            commit::commit_metadata(&key, [], None, Some(content_blob.clone()), None).to_blob();
        let parent = parent_blob.get_handle();
        let target_blob: Blob<SimpleArchive> =
            commit::commit_metadata(&key, [parent], None, None, None).to_blob();
        let target = target_blob.get_handle();
        let name_blob: Blob<LongString> = "weak-before-arrival".to_owned().to_blob();
        let name = name_blob.get_handle();
        let descriptor_blob = BranchPinDescriptor::blob(name);
        let descriptor = descriptor_blob.get_handle();
        let strong_blob = BranchPinDescriptor::strong_blob(name);
        let strong = strong_blob.get_handle();

        // Stage and flush the complete locally authored dependency chain before
        // publishing the assertion. A zero soft budget must not weaken any
        // member of the resulting strong closure.
        assert_eq!(yard.put::<LongString, _>(name_blob).unwrap(), name);
        assert_eq!(
            yard.put::<BranchPinDescriptor, _>(descriptor_blob).unwrap(),
            descriptor
        );
        assert_eq!(
            yard.put::<StrongPinDescriptor, _>(strong_blob).unwrap(),
            strong
        );
        assert_eq!(yard.put::<SimpleArchive, _>(content_blob).unwrap(), content);
        assert_eq!(yard.put::<SimpleArchive, _>(parent_blob).unwrap(), parent);
        assert_eq!(yard.put::<SimpleArchive, _>(target_blob).unwrap(), target);
        yard.flush().unwrap();
        yard.append_pin_assertion(sign_branch_assertion(
            &key,
            name,
            target,
            BranchRank::ROOT.successor().unwrap(),
        ))
        .unwrap();
        append_want(&mut yard, &key, strong);
        append_want(&mut yard, &key, descriptor);
        append_want(&mut yard, &key, name);
        append_want(&mut yard, &key, target);
        append_want(&mut yard, &key, parent);
        append_want(&mut yard, &key, content);
        let dead = yard
            .put::<RawBytes, _>(raw_blob(b"unasserted orphan"))
            .unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        let decoded_descriptor: Inline<Handle<UnknownBlob>> = reader.get(strong).unwrap();
        assert_eq!(decoded_descriptor.raw, descriptor.raw);
        let decoded_name: Inline<Handle<LongString>> = reader.get(descriptor).unwrap();
        assert_eq!(decoded_name, name);
        let restored_name: anybytes::View<str> = reader.get(name).unwrap();
        assert_eq!(&*restored_name, "weak-before-arrival");
        assert!(reader.get::<TribleSet, SimpleArchive>(target).is_ok());
        assert!(reader.get::<TribleSet, SimpleArchive>(parent).is_ok());
        assert!(reader.get::<TribleSet, SimpleArchive>(content).is_ok());
        assert!(matches!(
            get_raw(&reader, dead),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn branch_pin_assertions_survive_compact_reclaim_and_restart() {
        use crate::repo::branch_frontier::BranchResolution;
        use crate::repo::{PublishOutcome, Repository};
        use crate::trible::TribleSet;
        use ed25519_dalek::SigningKey;

        let config = YardConfig {
            want_budget: 0,
            strong_level_budget: 0,
            fanout: 1,
        };
        let (_dir, paths, yard) = yard_with_paths(2, config);
        let key = SigningKey::from_bytes(&[13; 32]);
        let mut repo = Repository::new(yard, key.clone(), TribleSet::new()).unwrap();
        let identity = repo.branch_identity("main");
        let name = identity.name();
        let descriptor = BranchPinDescriptor::descriptor_handle(name);
        let strong = BranchPinDescriptor::strong_blob(name).get_handle();
        let mut workspace = repo.create_workspace("main").unwrap();
        workspace.commit(TribleSet::new(), "first").unwrap();
        let head = workspace.head().unwrap();
        assert!(matches!(
            repo.push(&mut workspace).unwrap(),
            PublishOutcome::Published(_)
        ));
        let assertions_before = repo.storage_mut().pin_assertion_snapshot().unwrap();
        let dead = repo
            .storage_mut()
            .put::<RawBytes, _>(raw_blob(b"physically reclaimed"))
            .unwrap();

        // A zero durable budget moves the complete asserted closure down and
        // atomically rewrites the young segment that holds the assertion.
        repo.storage_mut().compact().unwrap();
        assert!(repo.storage().contains_in_generation(1, head));
        repo.storage_mut().reclaim().unwrap();
        repo.into_storage().close().unwrap();

        let mut reopened = Yard::open(paths, config).unwrap();
        assert_eq!(
            reopened.pin_assertion_snapshot().unwrap(),
            assertions_before,
            "physical rewrites must preserve the grow-only assertion set"
        );
        let reader = reopened.reader().unwrap();
        let decoded_descriptor: Inline<Handle<UnknownBlob>> = reader.get(strong).unwrap();
        assert_eq!(decoded_descriptor.raw, descriptor.raw);
        let decoded_name: Inline<Handle<LongString>> = reader.get(descriptor).unwrap();
        assert_eq!(decoded_name, name);
        let restored_name: anybytes::View<str> = reader.get(name).unwrap();
        assert_eq!(&*restored_name, "main");
        assert!(matches!(
            get_raw(&reader, dead),
            Err(YardGetError::NotFound)
        ));

        let mut repo = Repository::new(reopened, key, TribleSet::new()).unwrap();
        match repo.resolve(&identity).unwrap() {
            BranchResolution::Complete(frontier) => assert_eq!(frontier.tips(), &[head]),
            other => panic!("reopened asserted branch was not complete: {other:?}"),
        }
        let mut workspace = repo.pull(identity).unwrap();
        assert!(workspace.checkout(..).unwrap().is_empty());
        repo.into_storage().close().unwrap();
    }

    #[test]
    fn asserted_wants_never_veto_hard_reachability() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let key = SigningKey::from_bytes(&[9; 32]);
        // The child is both softly wanted and reachable from a hard root. A
        // zero soft budget may evict standalone wants, but cannot cut a hard
        // reachability edge.
        let child = Blob::<UnknownBlob>::new(Bytes::from_source(b"child".to_vec())).get_handle();
        append_want(&mut yard, &key, child);
        yard.put::<UnknownBlob, _>(Bytes::from_source(b"child".to_vec()))
            .unwrap();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(child.raw.to_vec()))
            .unwrap();

        yard.pin_strong(pin_id(2), parent).unwrap();
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(parent).is_ok());
        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(child).is_ok());
    }

    #[test]
    fn asserted_want_retains_only_its_exact_value_not_its_local_closure() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                want_budget: 1,
                ..YardConfig::default()
            },
        );
        let key = SigningKey::from_bytes(&[10; 32]);
        let child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"soft child".to_vec()))
            .unwrap();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(child.raw.to_vec()))
            .unwrap();
        append_want(&mut yard, &key, parent);

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();
        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(parent).is_ok());
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(child),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn hard_closure_ignores_a_missing_asserted_want() {
        let (_dir, mut yard) = yard_with(1, YardConfig::default());
        let key = SigningKey::from_bytes(&[10; 32]);
        let absent =
            Blob::<UnknownBlob>::new(Bytes::from_source(b"not stored".to_vec())).get_handle();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(absent.raw.to_vec()))
            .unwrap();

        yard.pin_strong(pin_id(3), parent).unwrap();
        append_want(&mut yard, &key, absent);

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(parent).is_ok());
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(absent),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn compaction_tenures_hard_roots_and_lets_soft_wants_descend() {
        let (_dir, mut yard) = yard_with(
            3,
            YardConfig {
                want_budget: 10,
                strong_level_budget: 0,
                fanout: 1,
            },
        );
        let strong = yard.put::<RawBytes, _>(raw_blob(b"tenured")).unwrap();
        let key = SigningKey::from_bytes(&[11; 32]);
        let wanted = Blob::<RawBytes>::new(raw_blob(b"cache")).get_handle();
        append_want(&mut yard, &key, wanted);
        yard.put::<RawBytes, _>(raw_blob(b"cache")).unwrap();
        yard.pin_strong(pin_id(4), strong).unwrap();

        yard.compact().unwrap();

        // With a zero strong budget everything overflows downward; weak now
        // rides the flow to the bottom alongside strong (it is not pinned to
        // the youngest generation), and stays there because it is within the
        // soft-want budget.
        assert!(!yard.contains_in_generation(0, strong));
        assert!(!yard.contains_in_generation(1, strong));
        assert!(yard.contains_in_generation(2, strong));
        assert!(!yard.contains_in_generation(0, wanted));
        assert!(!yard.contains_in_generation(1, wanted));
        assert!(yard.contains_in_generation(2, wanted));
    }

    #[test]
    fn compact_recycles_dumped_generations_without_a_separate_reclaim() {
        let (_dir, paths, mut yard) = yard_with_paths(
            2,
            YardConfig {
                want_budget: 0,
                strong_level_budget: 0,
                fanout: 1,
            },
        );
        // A strong blob lands in gen 0 and, with a zero budget, overflows on
        // compaction — the whole of gen 0 dumps into gen 1.
        let strong = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'S'; 512]))
            .unwrap();
        yard.pin_strong(pin_id(7), strong).unwrap();
        // Dead bytes physically present in gen 0, so there is genuinely
        // something for the merge to reclaim.
        let _dead = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'D'; 4096]))
            .unwrap();
        assert_eq!(pile_blob_count(&paths[0]), 2);
        let strong_before = {
            let reader = yard.reader().unwrap();
            get_raw(&reader, strong).unwrap()
        };

        yard.compact().unwrap();

        // No separate reclaim(): the merge itself recycled gen 0's pile, so it
        // is physically empty, while the live blob moved down to gen 1 and
        // stays readable.
        assert_eq!(pile_blob_count(&paths[0]), 0);
        assert!(yard.contains_in_generation(1, strong));
        let reader = yard.reader().unwrap();
        assert_eq!(get_raw(&reader, strong).unwrap(), strong_before);
    }

    #[test]
    fn superseded_strong_head_becomes_droppable() {
        let (_dir, mut yard) = yard_with(1, YardConfig::default());
        let old = yard.put::<RawBytes, _>(raw_blob(b"old")).unwrap();
        let pin = pin_id(5);

        yard.pin_strong(pin, old).unwrap();
        yard.collect().unwrap();
        assert_eq!(
            get_raw(&yard.reader().unwrap(), old).unwrap(),
            raw_blob(b"old")
        );

        let new = yard.put::<RawBytes, _>(raw_blob(b"new")).unwrap();
        yard.pin_strong(pin, new).unwrap();
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(matches!(get_raw(&reader, old), Err(YardGetError::NotFound)));
        assert_eq!(get_raw(&reader, new).unwrap(), raw_blob(b"new"));
    }

    #[test]
    fn reclaim_rewrites_generation_to_live_blobs_only() {
        let (_dir, paths, mut yard) = yard_with_paths(
            1,
            YardConfig {
                want_budget: 0,
                ..YardConfig::default()
            },
        );
        let live = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'L'; 512]))
            .unwrap();
        let evicted = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'E'; 4096]))
            .unwrap();

        yard.pin_strong(pin_id(6), live).unwrap();
        yard.collect().unwrap();
        let before_size = fs::metadata(&paths[0]).unwrap().len();
        let before_count = pile_blob_count(&paths[0]);
        let before_reader = yard.reader().unwrap();
        let live_before = get_raw(&before_reader, live).unwrap();

        assert!(matches!(
            get_raw(&before_reader, evicted),
            Err(YardGetError::NotFound)
        ));
        assert_eq!(before_count, 2);

        yard.reclaim().unwrap();

        let after_size = fs::metadata(&paths[0]).unwrap().len();
        let after_count = pile_blob_count(&paths[0]);
        let after_reader = yard.reader().unwrap();

        assert!(after_size < before_size);
        assert_eq!(after_count, 1);
        assert_eq!(get_raw(&after_reader, live).unwrap(), live_before);
        assert!(matches!(
            get_raw(&after_reader, evicted),
            Err(YardGetError::NotFound)
        ));

        let mut fresh_pile = Pile::open(&paths[0]).unwrap();
        fresh_pile.refresh().unwrap();
        let fresh_reader = fresh_pile.reader().unwrap();
        assert_eq!(
            fresh_reader.get::<Bytes, RawBytes>(live).unwrap(),
            live_before
        );
        assert!(matches!(
            fresh_reader.get::<Bytes, RawBytes>(evicted),
            Err(GetBlobError::BlobNotFound)
        ));
        drop(fresh_reader);
        fresh_pile.close().unwrap();

        yard.reclaim().unwrap();
        assert_eq!(fs::metadata(&paths[0]).unwrap().len(), after_size);
        assert_eq!(pile_blob_count(&paths[0]), after_count);
    }

    /// The amnesia regression: asserted wants are durable generic pin records,
    /// so reopening a yard preserves intent independently of cached content.
    #[test]
    fn yard_open_reloads_asserted_wants() {
        let (_dir, paths, mut yard) = yard_with_paths(2, YardConfig::default());
        let key = SigningKey::from_bytes(&[12; 32]);

        // A pure want: asserted while absent, never fetched.
        let want = Blob::<RawBytes>::new(raw_blob(b"still wanted after restart")).get_handle();
        append_want(&mut yard, &key, want);
        // A fetched cache entry: asserted while absent, then put.
        let cached = Blob::<RawBytes>::new(raw_blob(b"cached")).get_handle();
        append_want(&mut yard, &key, cached);
        yard.put::<RawBytes, _>(raw_blob(b"cached")).unwrap();

        drop(yard); // closes (and flushes) the generation piles

        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        let wants = wants_in_snapshot(
            &reopened.pin_assertion_snapshot().unwrap(),
            key.verifying_key(),
        );
        assert_eq!(
            wants,
            BTreeSet::from([want.transmute(), cached.transmute()])
        );

        // The reloaded asserted want still works as a soft retention root: the
        // cached blob survives collection under the default budget.
        reopened.collect().unwrap();
        let reader = reopened.reader().unwrap();
        assert_eq!(get_raw(&reader, cached).unwrap(), raw_blob(b"cached"));
    }

    /// A young-pile rewrite must preserve the generic assertion G-set even
    /// when the named blob is absent.
    #[test]
    fn asserted_wants_survive_reclaim() {
        let (_dir, paths, mut yard) = yard_with_paths(1, YardConfig::default());
        let key = SigningKey::from_bytes(&[13; 32]);

        let want = Blob::<RawBytes>::new(raw_blob(b"wanted, absent")).get_handle();
        append_want(&mut yard, &key, want);
        let cached = Blob::<RawBytes>::new(raw_blob(b"cached blob")).get_handle();
        append_want(&mut yard, &key, cached);
        yard.put::<RawBytes, _>(raw_blob(b"cached blob")).unwrap();

        // Reclaim copies the immutable assertion witnesses independently of
        // the live blob set.
        yard.reclaim().unwrap();

        drop(yard);
        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        assert_eq!(
            wants_in_snapshot(
                &reopened.pin_assertion_snapshot().unwrap(),
                key.verifying_key(),
            ),
            BTreeSet::from([want.transmute(), cached.transmute()]),
            "asserted wants were lost by reclaim rewrite"
        );
        let reader = reopened.reader().unwrap();
        assert_eq!(get_raw(&reader, cached).unwrap(), raw_blob(b"cached blob"));
    }

    /// The fail-loud posture: opening a yard whose generation pile has a
    /// corrupt tail must surface the corruption (naming the file) WITHOUT
    /// truncating anything; `Yard::amputate` is the explicit opt-in repair.
    #[test]
    fn open_fails_loud_on_corrupt_generation_without_truncating() {
        use std::io::Write;

        let (_dir, paths, mut yard) = yard_with_paths(1, YardConfig::default());
        let live = yard.put::<RawBytes, _>(raw_blob(b"survivor")).unwrap();
        drop(yard); // closes (and flushes) the generation pile

        // Corrupt the tail: append garbage that is not a valid record.
        {
            let mut file = fs::OpenOptions::new().append(true).open(&paths[0]).unwrap();
            file.write_all(&[0xFF; 64]).unwrap();
            file.sync_all().unwrap();
        }
        let corrupt_len = fs::metadata(&paths[0]).unwrap().len();

        // Fail-loud open: the corruption propagates, names the file, and
        // the file is NOT truncated.
        match Yard::open(paths.clone(), YardConfig::default()) {
            Err(YardOpenError::Pile { path, err }) => {
                assert_eq!(path, paths[0]);
                assert!(
                    matches!(err, ReadError::CorruptPile { .. }),
                    "expected CorruptPile, got: {err}"
                );
            }
            other => panic!("expected fail-loud corrupt open, got {other:?}"),
        }
        assert_eq!(
            fs::metadata(&paths[0]).unwrap().len(),
            corrupt_len,
            "fail-loud open must not truncate the generation pile"
        );

        // Explicit repair: amputate truncates the invalid tail and the
        // valid prefix stays readable.
        let mut repaired = Yard::amputate(paths.clone(), YardConfig::default()).unwrap();
        assert!(fs::metadata(&paths[0]).unwrap().len() < corrupt_len);
        let reader = repaired.reader().unwrap();
        assert_eq!(get_raw(&reader, live).unwrap(), raw_blob(b"survivor"));
    }

    /// Yard's PinStore impl: CAS semantics over the in-memory strong pins.
    #[test]
    fn yard_pinstore_cas_update() {
        let (_dir, mut yard) = yard_with(1, YardConfig::default());
        let h1 = yard.put::<RawBytes, _>(raw_blob(b"one")).unwrap();
        let h2 = yard.put::<RawBytes, _>(raw_blob(b"two")).unwrap();
        let pin = pin_id(9);

        assert!(matches!(
            yard.update(pin, None, Some(h1.transmute())).unwrap(),
            PushResult::Success()
        ));
        assert_eq!(yard.head(pin).unwrap(), Some(h1.transmute()));
        match yard
            .update(pin, Some(h2.transmute()), Some(h2.transmute()))
            .unwrap()
        {
            PushResult::Conflict(current) => assert_eq!(current, Some(h1.transmute())),
            other => panic!("expected conflict, got {other:?}"),
        }
        let ids: Vec<_> = yard.pins().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(ids, vec![pin]);
        assert!(matches!(
            yard.update(pin, Some(h1.transmute()), None).unwrap(),
            PushResult::Success()
        ));
        assert_eq!(yard.head(pin).unwrap(), None);
    }

    /// A Yard pin is not merely a process-local retention hint: its head and
    /// complete locally-present closure survive restart, generation movement,
    /// and both physical rewrite paths.
    #[test]
    fn strong_pin_and_reachable_closure_survive_reopen_compact_and_reclaim() {
        let config = YardConfig {
            want_budget: 0,
            strong_level_budget: 0,
            fanout: 1,
        };
        let (_dir, paths, mut yard) = yard_with_paths(2, config);
        let child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"reachable child".to_vec()))
            .unwrap();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(child.raw.to_vec()))
            .unwrap();
        let pin = pin_id(10);
        let parent_head: Inline<Handle<SimpleArchive>> = parent.transmute();

        assert!(matches!(
            yard.update(pin, None, Some(parent_head)).unwrap(),
            PushResult::Success()
        ));
        yard.close().unwrap();

        let mut reopened = Yard::open(paths.clone(), config).unwrap();
        assert_eq!(reopened.head(pin).unwrap(), Some(parent_head));
        reopened.compact().unwrap();
        assert!(reopened.contains_in_generation(1, parent));
        assert!(reopened.contains_in_generation(1, child));
        reopened.reclaim().unwrap();
        reopened.close().unwrap();

        let mut final_open = Yard::open(paths, config).unwrap();
        assert_eq!(final_open.head(pin).unwrap(), Some(parent_head));
        final_open.collect().unwrap();
        let reader = final_open.reader().unwrap();
        assert_eq!(
            reader
                .get::<Bytes, UnknownBlob>(parent)
                .expect("durable pin head was reclaimed"),
            Bytes::from_source(child.raw.to_vec())
        );
        assert_eq!(
            reader
                .get::<Bytes, UnknownBlob>(child)
                .expect("durable pin closure was reclaimed"),
            Bytes::from_source(b"reachable child".to_vec())
        );
    }

    /// Only the young generation's mutable-pin ledger is authoritative. A
    /// young tombstone must continue to mask a stale older head even after a
    /// compact rewrite drops the tombstone record itself.
    #[test]
    fn young_tombstone_cannot_resurrect_stale_older_pin_after_compact() {
        let config = YardConfig {
            want_budget: 0,
            strong_level_budget: 0,
            fanout: 1,
        };
        let (_dir, paths, yard) = yard_with_paths(2, config);
        yard.close().unwrap();

        let stale_pin = pin_id(11);
        let stale;
        {
            let mut old = Pile::open(&paths[1]).unwrap();
            old.refresh().unwrap();
            stale = old
                .put::<UnknownBlob, _>(Bytes::from_source(b"stale older root".to_vec()))
                .unwrap();
            assert!(matches!(
                old.update(stale_pin, None, Some(stale.transmute()))
                    .unwrap(),
                PushResult::Success()
            ));
            old.close().unwrap();
        }
        {
            let mut young = Pile::open(&paths[0]).unwrap();
            young.refresh().unwrap();
            let stale_head = Some(stale.transmute());
            assert!(matches!(
                young.update(stale_pin, None, stale_head).unwrap(),
                PushResult::Success()
            ));
            assert!(matches!(
                young.update(stale_pin, stale_head, None).unwrap(),
                PushResult::Success()
            ));
            young.close().unwrap();
        }

        let mut yard = Yard::open(paths.clone(), config).unwrap();
        assert_eq!(yard.head(stale_pin).unwrap(), None);

        // Keep a different strong root so compact must recycle the young
        // pile. Its rewrite deliberately emits only the current map, not a
        // tombstone for the deleted stale pin.
        let survivor = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"survivor".to_vec()))
            .unwrap();
        let survivor_pin = pin_id(12);
        yard.pin_strong(survivor_pin, survivor).unwrap();
        yard.compact().unwrap();
        yard.close().unwrap();

        // Prove the stale older record still physically exists; correctness
        // comes from the young-authority rule rather than accidental cleanup.
        {
            let mut old = Pile::open(&paths[1]).unwrap();
            old.refresh().unwrap();
            assert_eq!(old.head(stale_pin).unwrap(), Some(stale.transmute()));
            old.close().unwrap();
        }

        let mut reopened = Yard::open(paths, config).unwrap();
        assert_eq!(reopened.head(stale_pin).unwrap(), None);
        assert_eq!(
            reopened.head(survivor_pin).unwrap(),
            Some(survivor.transmute())
        );
        reopened.collect().unwrap();
        let reader = reopened.reader().unwrap();
        assert!(matches!(
            reader.get::<Bytes, UnknownBlob>(stale),
            Err(YardGetError::NotFound)
        ));
        assert_eq!(
            reader.get::<Bytes, UnknownBlob>(survivor).unwrap(),
            Bytes::from_source(b"survivor".to_vec())
        );
    }

    mod dst {
        use super::*;

        const GENERATIONS: usize = 4;
        const SEEDS: u64 = 50;
        const STEPS: usize = 64;
        const PIN_COUNT: usize = 8;

        type RawHandle = [u8; INLINE_LEN];

        #[derive(Debug, Clone)]
        struct Model {
            handles: Vec<RawHandle>,
            bytes: BTreeMap<RawHandle, Vec<u8>>,
            absent: Vec<RawHandle>,
            wants: BTreeSet<RawHandle>,
        }

        impl Model {
            fn new() -> Self {
                Self {
                    handles: Vec::new(),
                    bytes: BTreeMap::new(),
                    absent: Vec::new(),
                    wants: BTreeSet::new(),
                }
            }
        }

        #[derive(Clone, Debug, PartialEq, Eq)]
        struct FinalState {
            live_by_generation: Vec<Vec<RawHandle>>,
            readable: Vec<RawHandle>,
        }

        #[derive(Clone, Copy, Debug)]
        struct SplitMix64 {
            state: u64,
        }

        impl SplitMix64 {
            fn new(seed: u64) -> Self {
                Self { state: seed }
            }

            fn next_u64(&mut self) -> u64 {
                self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = self.state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^ (z >> 31)
            }

            fn index(&mut self, len: usize) -> usize {
                (self.next_u64() as usize) % len
            }

            fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
                self.next_u64() % denominator < numerator
            }

            fn fill(&mut self, bytes: &mut [u8]) {
                for chunk in bytes.chunks_mut(8) {
                    let random = self.next_u64().to_le_bytes();
                    chunk.copy_from_slice(&random[..chunk.len()]);
                }
            }
        }

        fn unknown(raw: RawHandle) -> Inline<Handle<UnknownBlob>> {
            Inline::<Handle<UnknownBlob>>::new(raw)
        }

        fn pin_id(index: usize) -> Id {
            Id::new([(index as u8).wrapping_add(1); 16]).unwrap()
        }

        fn live_sets(yard: &Yard) -> Vec<BTreeSet<RawHandle>> {
            yard.generations
                .iter()
                .map(|generation| {
                    generation
                        .segments
                        .iter()
                        .flat_map(|s| s.live.clone().into_iter())
                        .collect()
                })
                .collect()
        }

        fn live_union(yard: &Yard) -> BTreeSet<RawHandle> {
            live_sets(yard).into_iter().flatten().collect()
        }

        fn strong_roots(yard: &Yard) -> Vec<RawHandle> {
            (&yard.strong_pins)
                .into_iter()
                .filter_map(|pin| yard.strong_pins.get(pin).copied())
                .map(|handle| handle.raw)
                .collect()
        }

        fn budgeted_wants(
            wants: &BTreeSet<RawHandle>,
            present: &BTreeSet<RawHandle>,
            budget: usize,
        ) -> BTreeSet<RawHandle> {
            wants
                .iter()
                .take(budget)
                .filter(|raw| present.contains(*raw))
                .copied()
                .collect()
        }

        fn child_chunks(bytes: &[u8]) -> impl Iterator<Item = RawHandle> + '_ {
            bytes.chunks_exact(INLINE_LEN).map(|chunk| {
                let mut raw = [0u8; INLINE_LEN];
                raw.copy_from_slice(chunk);
                raw
            })
        }

        fn model_strong_keep(
            roots: &[RawHandle],
            present: &BTreeSet<RawHandle>,
            model: &Model,
        ) -> BTreeSet<RawHandle> {
            let mut queue = VecDeque::new();
            for root in roots {
                queue.push_back(*root);
            }

            let mut keep = BTreeSet::new();
            while let Some(raw) = queue.pop_front() {
                if !keep.insert(raw) || !present.contains(&raw) {
                    continue;
                }

                let Some(bytes) = model.bytes.get(&raw) else {
                    continue;
                };

                for child in child_chunks(bytes) {
                    if present.contains(&child)
                        && model.bytes.contains_key(&child)
                        && !keep.contains(&child)
                    {
                        queue.push_back(child);
                    }
                }
            }

            keep
        }

        fn expected_live_after_collect(yard: &Yard, model: &Model) -> BTreeSet<RawHandle> {
            let present = live_union(yard);
            let strong_keep = model_strong_keep(&strong_roots(yard), &present, model);
            let want_keep = budgeted_wants(&model.wants, &present, yard.config.want_budget);

            present
                .into_iter()
                .filter(|raw| strong_keep.contains(raw) || want_keep.contains(raw))
                .collect()
        }

        fn assert_readable_bytes(
            reader: &YardReader,
            raw: RawHandle,
            expected: &[u8],
            seed: u64,
            step: usize,
        ) {
            let actual = reader
                .get_local::<Bytes, UnknownBlob>(unknown(raw))
                .unwrap_or_else(|| {
                    panic!("seed {seed} step {step}: live handle {raw:02X?} was not readable")
                })
                .unwrap_or_else(|err| {
                    panic!("seed {seed} step {step}: live handle {raw:02X?} errored: {err}")
                });
            assert_eq!(
                actual.as_ref(),
                expected,
                "seed {seed} step {step}: readable bytes changed for {raw:02X?}"
            );
        }

        fn assert_general_invariants(yard: &mut Yard, model: &Model, seed: u64, step: usize) {
            let actual_wants = all_wants_in_snapshot(&yard.pin_assertion_snapshot().unwrap())
                .into_iter()
                .map(|handle| handle.raw)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual_wants, model.wants,
                "seed {seed} step {step}: asserted want G-set diverged"
            );
            let reader = yard.reader().unwrap();
            let live = live_union(yard);
            let strong_keep = model_strong_keep(&strong_roots(yard), &live, model);

            for raw in strong_keep.intersection(&live) {
                let expected = model
                    .bytes
                    .get(raw)
                    .unwrap_or_else(|| panic!("seed {seed} step {step}: unknown live handle"));
                assert_readable_bytes(&reader, *raw, expected, seed, step);
            }

            for raw in &live {
                let expected = model.bytes.get(raw).unwrap_or_else(|| {
                    panic!("seed {seed} step {step}: live set has unknown blob")
                });
                assert_readable_bytes(&reader, *raw, expected, seed, step);
                let _ = reader.children(unknown(*raw));
            }

            for raw in model.bytes.keys().filter(|raw| !live.contains(*raw)) {
                assert!(
                    reader
                        .get_local::<Bytes, UnknownBlob>(unknown(*raw))
                        .is_none(),
                    "seed {seed} step {step}: non-live handle {raw:02X?} was readable"
                );
            }

            for raw in &model.absent {
                assert!(
                    reader
                        .get_local::<Bytes, UnknownBlob>(unknown(*raw))
                        .is_none(),
                    "seed {seed} step {step}: absent handle {raw:02X?} became readable"
                );
                assert!(
                    reader.children(unknown(*raw)).is_empty(),
                    "seed {seed} step {step}: absent handle {raw:02X?} had children"
                );
            }
        }

        fn assert_exact_collect_result(
            yard: &mut Yard,
            expected: &BTreeSet<RawHandle>,
            model: &Model,
            seed: u64,
            step: usize,
        ) {
            let actual = live_union(yard);
            assert_eq!(
                &actual, expected,
                "seed {seed} step {step}: live union after collection did not equal keep set"
            );
            assert_general_invariants(yard, model, seed, step);
        }

        fn snapshot_readable(yard: &mut Yard) -> BTreeMap<RawHandle, Vec<u8>> {
            let reader = yard.reader().unwrap();
            live_union(yard)
                .into_iter()
                .filter_map(|raw| {
                    reader
                        .get_local::<Bytes, UnknownBlob>(unknown(raw))
                        .map(|result| (raw, result.unwrap().as_ref().to_vec()))
                })
                .collect()
        }

        fn assert_reclaim_preserved(
            yard: &mut Yard,
            before: &BTreeMap<RawHandle, Vec<u8>>,
            model: &Model,
            seed: u64,
            step: usize,
        ) {
            let reader = yard.reader().unwrap();
            let live = live_union(yard);
            for (raw, bytes) in before {
                assert!(
                    live.contains(raw),
                    "seed {seed} step {step}: reclaim removed live handle {raw:02X?}"
                );
                assert_readable_bytes(&reader, *raw, bytes, seed, step);
            }
            for raw in model.bytes.keys().filter(|raw| !live.contains(*raw)) {
                assert!(
                    reader
                        .get_local::<Bytes, UnknownBlob>(unknown(*raw))
                        .is_none(),
                    "seed {seed} step {step}: reclaim exposed non-live handle {raw:02X?}"
                );
            }
        }

        fn fresh_absent_handle(rng: &mut SplitMix64, model: &mut Model) -> RawHandle {
            let mut bytes = vec![0u8; 48];
            rng.fill(&mut bytes);
            let handle = Blob::<UnknownBlob>::new(Bytes::from_source(bytes)).get_handle();
            model.absent.push(handle.raw);
            handle.raw
        }

        fn choose_known_or_absent(rng: &mut SplitMix64, model: &mut Model) -> RawHandle {
            if !model.handles.is_empty() && rng.chance(3, 4) {
                model.handles[rng.index(model.handles.len())]
            } else {
                fresh_absent_handle(rng, model)
            }
        }

        fn put_fresh_blob(
            yard: &mut Yard,
            model: &mut Model,
            rng: &mut SplitMix64,
            seed: u64,
            step: usize,
        ) {
            let mut bytes = Vec::new();
            let mut unique = [0u8; INLINE_LEN];
            unique[..8].copy_from_slice(&seed.to_le_bytes());
            unique[8..16].copy_from_slice(&(step as u64).to_le_bytes());
            unique[16..24].copy_from_slice(&rng.next_u64().to_le_bytes());
            unique[24..32].copy_from_slice(&rng.next_u64().to_le_bytes());
            bytes.extend_from_slice(&unique);

            let child_count = if model.handles.is_empty() {
                0
            } else {
                rng.index(4)
            };
            for _ in 0..child_count {
                let child = choose_known_or_absent(rng, model);
                bytes.extend_from_slice(&child);
            }

            let noise_len = rng.index(17);
            let mut noise = vec![0u8; noise_len];
            rng.fill(&mut noise);
            bytes.extend_from_slice(&noise);

            let blob = Blob::<UnknownBlob>::new(Bytes::from_source(bytes.clone()));
            let expected = blob.get_handle();
            let handle = if rng.chance(2, 3) {
                yard.put::<UnknownBlob, _>(blob).unwrap()
            } else {
                let level = rng.index(GENERATIONS);
                yard.put_in_generation::<UnknownBlob, _>(level, blob)
                    .unwrap()
            };
            assert_eq!(handle.raw, expected.raw);

            model.bytes.entry(handle.raw).or_insert(bytes);
            if !model.handles.contains(&handle.raw) {
                model.handles.push(handle.raw);
            }
        }

        fn run_one(seed: u64) -> FinalState {
            let (_dir, mut yard) = yard_with(
                GENERATIONS,
                YardConfig {
                    want_budget: 3,
                    strong_level_budget: 2,
                    fanout: 2,
                },
            );
            let mut rng = SplitMix64::new(seed);
            let mut model = Model::new();
            let want_key = SigningKey::from_bytes(&[42; 32]);

            for step in 0..STEPS {
                match rng.index(9) {
                    0 | 1 => put_fresh_blob(&mut yard, &mut model, &mut rng, seed, step),
                    2 => {
                        if !model.handles.is_empty() {
                            let pin = pin_id(rng.index(PIN_COUNT));
                            let raw = model.handles[rng.index(model.handles.len())];
                            yard.pin_strong(pin, unknown(raw)).unwrap();
                        }
                    }
                    3 => yard.unpin_strong(pin_id(rng.index(PIN_COUNT))).unwrap(),
                    4 => {
                        let raw = choose_known_or_absent(&mut rng, &mut model);
                        append_want(&mut yard, &want_key, unknown(raw));
                        model.wants.insert(raw);
                    }
                    5 => {
                        let raw = choose_known_or_absent(&mut rng, &mut model);
                        let reader = yard.reader().unwrap();
                        let result = reader.get::<Bytes, UnknownBlob>(unknown(raw));
                        if !live_union(&yard).contains(&raw) {
                            assert!(
                                matches!(result, Err(YardGetError::NotFound)),
                                "seed {seed} step {step}: absent get did not miss cleanly"
                            );
                        }
                    }
                    6 => {
                        let expected = expected_live_after_collect(&yard, &model);
                        yard.collect().unwrap();
                        assert_exact_collect_result(&mut yard, &expected, &model, seed, step);
                    }
                    7 => {
                        let expected = expected_live_after_collect(&yard, &model);
                        yard.compact().unwrap();
                        assert_exact_collect_result(&mut yard, &expected, &model, seed, step);
                    }
                    8 => {
                        let before = snapshot_readable(&mut yard);
                        yard.reclaim().unwrap();
                        assert_reclaim_preserved(&mut yard, &before, &model, seed, step);
                    }
                    _ => unreachable!(),
                }

                assert_general_invariants(&mut yard, &model, seed, step);
            }

            let reader = yard.reader().unwrap();
            let mut live_by_generation = live_sets(&yard)
                .into_iter()
                .map(|set| set.into_iter().collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for generation in &mut live_by_generation {
                generation.sort();
            }
            let mut readable = live_union(&yard)
                .into_iter()
                .filter(|raw| {
                    reader
                        .get_local::<Bytes, UnknownBlob>(unknown(*raw))
                        .is_some()
                })
                .collect::<Vec<_>>();
            readable.sort();

            FinalState {
                live_by_generation,
                readable,
            }
        }

        #[test]
        fn seeded_yard_property_sequences() {
            for seed in 0..SEEDS {
                run_one(0xC0DE_0000_0000_0000 ^ seed);
            }
        }

        #[test]
        fn seeded_yard_property_sequences_are_deterministic() {
            for seed in [0, 13, 49] {
                let seed = 0xD57D_0000_0000_0000 ^ seed;
                assert_eq!(run_one(seed), run_one(seed), "seed {seed} diverged");
            }
        }

        #[test]
        fn seeded_yard_property_sequences_cover_resident_and_absent_wants() {
            for seed in [0, 2, 7, 13, 31, 49] {
                run_one(0xC0DE_0000_0000_0000 ^ seed);
            }
        }

        #[test]
        fn asserted_want_does_not_downgrade_a_tenured_hard_root() {
            let (_dir, mut yard) = yard_with(
                3,
                YardConfig {
                    want_budget: 1,
                    strong_level_budget: 0,
                    fanout: 1,
                },
            );
            let tenured = yard
                .put::<UnknownBlob, _>(Bytes::from_source(b"tenured then weak".to_vec()))
                .unwrap();

            yard.pin_strong(pin_id(0), tenured).unwrap();
            yard.compact().unwrap();
            assert!(yard.contains_in_generation(2, tenured));

            let key = SigningKey::from_bytes(&[43; 32]);
            append_want(&mut yard, &key, tenured);
            yard.compact().unwrap();

            assert!(
                yard.contains_in_generation(2, tenured),
                "soft demand must not weaken a hard root"
            );
        }
    }
}
