//! Generational collection of piles for lazy-retention blob storage.
//!
//! A [`Yard`] keeps an ordered young-to-old sequence of [`Pile`](super::pile::Pile)
//! generations. Writes land in the youngest generation, reads search the union
//! of each generation's live PATCH set, and retention/compaction update those
//! PATCH sets without changing Pile's append-only storage contract. Call
//! [`Yard::reclaim`] after collection when the logically evicted blobs should
//! also be physically removed from disk.

use std::cmp::Reverse;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anybytes::Bytes;

use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use crate::id::{Id, RawId};
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding, INLINE_LEN};
use crate::patch::{Entry, IdentitySchema, PATCH};

use crate::prelude::blobencodings::SimpleArchive;

use super::pile::{GetBlobError, InsertError, Pile, PileReader, ReadError, UpdateBranchError};
use super::{
    reachable, transfer, BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut,
    PinStore, PushResult, StorageClose, TransferError, WeakPinStore,
};

type HandleSet = PATCH<INLINE_LEN, IdentitySchema>;
type StrongPins = PATCH<16, IdentitySchema, Inline<Handle<UnknownBlob>>>;
type WeakPins = PATCH<INLINE_LEN, IdentitySchema, WeakPin>;

#[derive(Debug, Clone, Copy)]
struct WeakPin {
    last_used: u64,
}

#[derive(Debug, Default)]
struct WeakState {
    pins: WeakPins,
    clock: u64,
}

impl WeakState {
    fn pin(&mut self, handle: Inline<Handle<UnknownBlob>>) {
        self.clock = self.clock.wrapping_add(1).max(1);
        let entry = Entry::with_value(
            &handle.raw,
            WeakPin {
                last_used: self.clock,
            },
        );
        self.pins.replace(&entry);
    }

    fn unpin(&mut self, raw: &[u8; INLINE_LEN]) {
        self.pins.remove(raw);
    }

    fn contains(&self, raw: &[u8; INLINE_LEN]) -> bool {
        self.pins.get(raw).is_some()
    }

    fn trim_to_present_budget(&mut self, present: &HandleSet, budget: usize) -> HandleSet {
        let mut candidates = Vec::new();
        for raw in &self.pins {
            if present.get(raw).is_some() {
                let pin = *self
                    .pins
                    .get(raw)
                    .expect("key from PATCH iterator must resolve in the same PATCH");
                candidates.push((*raw, pin.last_used));
            }
        }

        candidates.sort_by_key(|(_, last_used)| Reverse(*last_used));

        let mut retained = WeakPins::new();
        let mut handles = HandleSet::new();
        for (raw, last_used) in candidates.into_iter().take(budget) {
            retained.replace(&Entry::with_value(&raw, WeakPin { last_used }));
            handles.insert(&Entry::new(&raw));
        }
        self.pins = retained;
        handles
    }
}

#[derive(Debug, Clone, Copy)]
pub struct YardConfig {
    /// Maximum number of weak-pinned blobs retained in the young cache.
    pub weak_budget: usize,
    /// Strong survivor budget for the youngest level.
    pub strong_level_budget: usize,
    /// Per-level strong budget multiplier.
    pub fanout: usize,
}

impl Default for YardConfig {
    fn default() -> Self {
        Self {
            weak_budget: 1024,
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
    weak_state: Arc<Mutex<WeakState>>,
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
            weak_state: Arc::new(Mutex::new(WeakState::default())),
        })
    }

    /// Open an existing yard and treat all blobs in each pile as live.
    ///
    /// Fails loud on corruption: a generation pile with an invalid tail
    /// surfaces as [`YardOpenError::Pile`] naming the file, and **nothing is
    /// truncated**. Repair is an explicit opt-in via [`Yard::restore`]
    /// (mirroring [`Pile::refresh`] vs [`Pile::restore`]).
    ///
    /// The weak-pin state is rebuilt from the durable weak-pin markers found
    /// in the generation piles (old to young, so the young generation's
    /// markers override older ones), fixing the restart amnesia the previous
    /// in-memory-only weak state had.
    pub fn open<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        Self::open_impl(paths, config, false)
    }

    /// Open an existing yard, **repairing** each generation pile first:
    /// any invalid tail (for example a torn write left by a crash) is
    /// truncated back to the last valid record, exactly like
    /// [`Pile::restore`]. This is the explicit opt-in counterpart to the
    /// fail-loud [`Yard::open`] — reach for it only after `open` reported
    /// corruption and losing the invalid tail is acceptable.
    pub fn restore<P>(
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
                pile.restore()
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
        // Reload the durable weak pins. Iterate old -> young so a young
        // marker (re-)pins last and wins the LRU recency slot; each pile's
        // own set is already LWW-resolved by its log order. (In practice
        // markers are only ever written to the young generation's pile.)
        let mut weak_state = WeakState::default();
        for generation in generations.iter_mut().rev() {
            for segment in &mut generation.segments {
                for marker in segment.pile_mut().weak_pins().map_err(update_err_io)? {
                    weak_state.pin(marker.map_err(update_err_io)?);
                }
            }
        }
        Ok(Self {
            generations,
            config,
            strong_pins: StrongPins::new(),
            weak_state: Arc::new(Mutex::new(weak_state)),
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
        self.generations.get(level).is_some_and(|g| {
            g.segments
                .iter()
                .any(|s| s.live.get(&handle.raw).is_some())
        })
    }

    /// Strongly pin a blob as the current head for `pin`.
    pub fn pin_strong<S>(&mut self, pin: Id, handle: Inline<Handle<S>>)
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
        let raw: RawId = pin.into();
        self.strong_pins.replace(&Entry::with_value(&raw, handle));
    }

    /// Remove a strong pin.
    pub fn unpin_strong(&mut self, pin: Id) {
        let raw: RawId = pin.into();
        self.strong_pins.remove(&raw);
    }

    /// Returns whether `handle` is resident (live) in any generation.
    fn is_resident(&self, handle: &Inline<Handle<UnknownBlob>>) -> bool {
        self.generations.iter().any(|generation| {
            generation
                .segments
                .iter()
                .any(|s| s.live.get(&handle.raw).is_some())
        })
    }

    /// Re-append the surviving weak-pin markers to the young generation's
    /// pile. A pile rewrite ([`reclaim_generation`]) transfers only live
    /// blobs, so it drops the weak-pin marker records along with the dead
    /// bytes; whenever the young pile is rewritten the current weak set must
    /// be re-recorded (surviving pins re-recorded, evicted ones dropped —
    /// eviction already removed them from the in-memory set).
    fn rerecord_weak_markers(&mut self) -> Result<(), std::io::Error> {
        let pins: Vec<Inline<Handle<UnknownBlob>>> = {
            let weak_state = self.weak_state.lock().expect("weak pin mutex poisoned");
            (&weak_state.pins)
                .into_iter()
                .map(|raw| Inline::<Handle<UnknownBlob>>::new(*raw))
                .collect()
        };
        let pile = self.generations[0].active_mut().pile_mut();
        for handle in pins {
            pile.pin_weak(handle).map_err(|err| match err {
                UpdateBranchError::IoError(io) => io,
            })?;
        }
        pile.flush().map_err(|err| match err {
            super::pile::FlushError::IoError(io) => io,
        })?;
        Ok(())
    }

    /// Recompute the keep set and logically collect cold weak pins and orphans.
    pub fn collect(&mut self) -> Result<(), YardCollectError> {
        let reader = self.reader().map_err(YardCollectError::Reader)?;
        let strong_keep = self.strong_keep_set(&reader);
        let present = reader.live_set();
        let weak_keep = self
            .weak_state
            .lock()
            .expect("weak pin mutex poisoned")
            .trim_to_present_budget(&present, self.config.weak_budget);

        let mut keep = strong_keep;
        keep.union(weak_keep);
        for generation in &mut self.generations {
            for segment in &mut generation.segments {
                segment.live = segment.live.intersect(&keep);
            }
        }
        Ok(())
    }

    /// Run one compaction pass.
    ///
    /// Strong survivors descend when a level exceeds its strong budget. Weak
    /// pins are only retained in the young generation and never copied down.
    pub fn compact(&mut self) -> Result<(), YardCollectError> {
        self.collect()?;
        let last = self.generations.len().saturating_sub(1);
        let mut dumped = Vec::new();

        {
            let reader = self.reader().map_err(YardCollectError::Reader)?;
            let strong_keep = self.strong_keep_set(&reader);

            for level in 0..last {
                let strong_here =
                    self.generations[level].segments[0].live.intersect(&strong_keep);
                if strong_here.len() as usize <= self.strong_budget_for(level) {
                    continue;
                }

                // Overflow: dump the whole tier down — strong *and* weak
                // survivors. `collect()` above already dropped dead, so the
                // segment's `live` is exactly the survivors. Weak descends to
                // use space in lower tiers rather than being pinned to the
                // youngest generation; it stays evictable everywhere and is
                // dropped by the weak budget under pressure.
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
            // The rewrite dropped the young pile's weak-pin markers along
            // with its dead bytes; re-record the surviving weak set.
            if level == 0 {
                self.rerecord_weak_markers()
                    .map_err(YardCollectError::WeakMarkers)?;
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
            // The rewrite dropped the young pile's weak-pin markers along
            // with its dead bytes; re-record the surviving weak set so the
            // pins stay durable (evicted ones are simply not re-recorded).
            if level == 0 {
                self.rerecord_weak_markers()
                    .map_err(YardReclaimError::WeakMarkers)?;
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
        let weak_state = self.weak_state.lock().expect("weak pin mutex poisoned");
        let roots: Vec<_> = (&self.strong_pins)
            .into_iter()
            .filter_map(|pin| self.strong_pins.get(pin).copied())
            .filter(|handle| !weak_state.contains(&handle.raw))
            .collect();
        drop(weak_state);

        let mut keep = HandleSet::new();
        for handle in reachable(reader, roots) {
            let weak_state = self.weak_state.lock().expect("weak pin mutex poisoned");
            if weak_state.contains(&handle.raw) {
                continue;
            }
            drop(weak_state);
            keep.insert(&Entry::new(&handle.raw));
        }
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

impl PinStore for Yard {
    type PinsError = Infallible;
    type HeadError = Infallible;
    type UpdateError = Infallible;

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
        match new {
            Some(new) => self.pin_strong(id, new),
            None => self.unpin_strong(id),
        }
        Ok(PushResult::Success())
    }
}

impl WeakPinStore for Yard {
    type WeakPinError = UpdateBranchError;

    type WeakListIter<'a> = std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, UpdateBranchError>>;

    /// Weakly pin a blob: refresh its LRU recency in memory AND persist a
    /// weak-pin marker to the young generation's pile, so the want survives
    /// a restart ([`Yard::open`] reloads it).
    ///
    /// A weak pin is the demand-born want-signal for content you *lack*
    /// (minted on a get-miss). Weak-pinning a blob you already hold
    /// (resident in any generation) is a **no-op** — nothing is recorded:
    /// weak entries originate only from demand, never from tagging resident
    /// content. (Releasing resident content you no longer need durably is a
    /// separate concern — the local→distributed handoff — not wired here
    /// yet.)
    fn pin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
        if self.is_resident(&handle) {
            return Ok(());
        }
        self.generations[0]
            .active_mut()
            .pile_mut()
            .pin_weak::<UnknownBlob>(handle)?;
        self.weak_state
            .lock()
            .expect("weak pin mutex poisoned")
            .pin(handle);
        Ok(())
    }

    /// Retract a weak pin: remove it from the in-memory weak state and
    /// persist a weak-unpin marker to the young generation's pile
    /// (last-writer-wins against any earlier weak-pin marker).
    fn unpin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations[0]
            .active_mut()
            .pile_mut()
            .unpin_weak::<UnknownBlob>(handle)?;
        self.weak_state
            .lock()
            .expect("weak pin mutex poisoned")
            .unpin(&handle.raw);
        Ok(())
    }

    fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError> {
        let items: Vec<Result<Inline<Handle<UnknownBlob>>, UpdateBranchError>> = {
            let weak_state = self.weak_state.lock().expect("weak pin mutex poisoned");
            (&weak_state.pins)
                .into_iter()
                .map(|raw| Ok(Inline::<Handle<UnknownBlob>>::new(*raw)))
                .collect()
        };
        Ok(items.into_iter())
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
        Ok(YardReader {
            generations,
            weak_state: self.weak_state.clone(),
        })
    }
}

impl super::StorageFlush for Yard {
    type Error = super::pile::FlushError;

    /// Flush every open generation pile. Weak-pin markers and fresh
    /// writes land in the young generation, but older generations can
    /// hold unsynced rewrites from `reclaim`/`compact`, so sync them all.
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
    weak_state: Arc<Mutex<WeakState>>,
}

impl YardReader {
    fn live_set(&self) -> HandleSet {
        let mut live = HandleSet::new();
        for generation in &self.generations {
            live.union(generation.live.clone());
        }
        live
    }

    /// Union read across generations (young -> old) that does NOT mint a
    /// demand-born weak pin on a miss; returns `None` on a clean miss.
    /// Speculative / structural reads (reference discovery via
    /// `children`) use this so they never pollute the weak set with
    /// wants for non-existent hashes. The public `get` layers the
    /// demand-born want on top of it.
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
        match self.get_local::<T, S>(handle) {
            Some(result) => result,
            None => {
                // An *intentional* read that missed is a demand-born
                // "want" — mint the weak pin so the sync daemon can fetch
                // it. Speculative scans use `get_local` and never land here.
                self.weak_state
                    .lock()
                    .expect("weak pin mutex poisoned")
                    .pin(handle.transmute());
                Err(YardGetError::NotFound)
            }
        }
    }
}

impl BlobChildren for YardReader {
    fn children(&self, handle: Inline<Handle<UnknownBlob>>) -> Vec<Inline<Handle<UnknownBlob>>> {
        // Structural scan: use the non-minting read so reference
        // discovery never floods the weak set with speculative wants.
        let Some(Ok(blob)) = self.get_local::<Blob<UnknownBlob>, UnknownBlob>(handle) else {
            return Vec::new();
        };
        let bytes = blob.bytes.as_ref();
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + INLINE_LEN <= bytes.len() {
            let mut raw = [0u8; INLINE_LEN];
            raw.copy_from_slice(&bytes[offset..offset + INLINE_LEN]);

            if self
                .weak_state
                .lock()
                .expect("weak pin mutex poisoned")
                .contains(&raw)
            {
                offset += INLINE_LEN;
                continue;
            }

            let candidate = Inline::<Handle<UnknownBlob>>::new(raw);
            if matches!(self.get_local::<Bytes, UnknownBlob>(candidate), Some(Ok(_))) {
                result.push(candidate);
            }
            offset += INLINE_LEN;
        }
        result
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

fn update_err_io(err: UpdateBranchError) -> YardOpenError {
    match err {
        UpdateBranchError::IoError(io) => YardOpenError::Io(io),
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
    /// [`Yard::restore`] if losing the tail is acceptable.
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
                write!(f, "failed to open yard generation pile {}: {err}", path.display())
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
    Reader(YardReaderError),
    Transfer(TransferError<Infallible, YardGetError<Infallible>, InsertError>),
    Flush(super::pile::FlushError),
    Reclaim(YardReclaimError),
    WeakMarkers(std::io::Error),
}

impl fmt::Display for YardCollectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reader(err) => write!(f, "failed to create yard reader: {err}"),
            Self::Transfer(err) => write!(f, "failed to compact yard generation: {err}"),
            Self::Flush(err) => write!(f, "failed to flush yard generation pile: {err}"),
            Self::Reclaim(err) => {
                write!(f, "failed to recycle compacted yard generation: {err}")
            }
            Self::WeakMarkers(err) => {
                write!(f, "failed to re-record weak-pin markers: {err}")
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
    Transfer(TransferError<Infallible, GetBlobError<Infallible>, InsertError>),
    Close(super::pile::FlushError),
    WeakMarkers(std::io::Error),
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
            Self::Transfer(err) => write!(f, "failed to copy live yard blobs: {err}"),
            Self::Close(err) => write!(f, "failed to close yard generation pile: {err}"),
            Self::WeakMarkers(err) => {
                write!(f, "failed to re-record weak-pin markers: {err}")
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
    use crate::blob::encodings::rawbytes::RawBytes;
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
    fn strong_keep_and_weak_evict_gc() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                weak_budget: 0,
                ..YardConfig::default()
            },
        );
        let strong = yard.put::<RawBytes, _>(raw_blob(b"strong")).unwrap();
        // demand-born weak: wanted while absent, then fetched, then LRU-
        // evicted under a zero budget — a genuine cache eviction, not an
        // orphan sweep.
        let weak = Blob::<RawBytes>::new(raw_blob(b"weak")).get_handle();
        yard.pin_weak(weak).unwrap();
        yard.put::<RawBytes, _>(raw_blob(b"weak")).unwrap();

        yard.pin_strong(pin_id(1), strong);
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert_eq!(get_raw(&reader, strong).unwrap(), raw_blob(b"strong"));
        assert!(matches!(
            get_raw(&reader, weak),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn weak_veto_overrides_strong_reachability() {
        let (_dir, mut yard) = yard_with(
            1,
            YardConfig {
                weak_budget: 0,
                ..YardConfig::default()
            },
        );
        // `child` enters the cache the demand-born way: weak-pinned while
        // absent (the want), then fetched. It is reachable from a strong
        // parent, yet the weak veto still makes it evictable.
        let child =
            Blob::<UnknownBlob>::new(Bytes::from_source(b"child".to_vec())).get_handle();
        yard.pin_weak(child).unwrap();
        yard.put::<UnknownBlob, _>(Bytes::from_source(b"child".to_vec()))
            .unwrap();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(child.raw.to_vec()))
            .unwrap();

        yard.pin_strong(pin_id(2), parent);
        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(parent).is_ok());
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(child),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn hole_safe_walk_prunes_weak_absent_child() {
        let (_dir, mut yard) = yard_with(1, YardConfig::default());
        let absent =
            Blob::<UnknownBlob>::new(Bytes::from_source(b"not stored".to_vec())).get_handle();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(absent.raw.to_vec()))
            .unwrap();

        yard.pin_strong(pin_id(3), parent);
        yard.pin_weak(absent).unwrap();

        yard.collect().unwrap();
        let reader = yard.reader().unwrap();

        assert!(reader.get::<Blob<UnknownBlob>, UnknownBlob>(parent).is_ok());
        assert!(matches!(
            reader.get::<Blob<UnknownBlob>, UnknownBlob>(absent),
            Err(YardGetError::NotFound)
        ));
    }

    #[test]
    fn compaction_tenures_strong_and_lets_weak_descend() {
        let (_dir, mut yard) = yard_with(
            3,
            YardConfig {
                weak_budget: 10,
                strong_level_budget: 0,
                fanout: 1,
            },
        );
        let strong = yard.put::<RawBytes, _>(raw_blob(b"tenured")).unwrap();
        // `weak` is demand-born: wanted while absent, then fetched, so it is
        // a genuine cache entry — not a resident downgrade, which no-ops.
        let weak = Blob::<RawBytes>::new(raw_blob(b"cache")).get_handle();
        yard.pin_weak(weak).unwrap();
        yard.put::<RawBytes, _>(raw_blob(b"cache")).unwrap();
        yard.pin_strong(pin_id(4), strong);

        yard.compact().unwrap();

        // With a zero strong budget everything overflows downward; weak now
        // rides the flow to the bottom alongside strong (it is not pinned to
        // the youngest generation), and stays there because it is within the
        // weak budget.
        assert!(!yard.contains_in_generation(0, strong));
        assert!(!yard.contains_in_generation(1, strong));
        assert!(yard.contains_in_generation(2, strong));
        assert!(!yard.contains_in_generation(0, weak));
        assert!(!yard.contains_in_generation(1, weak));
        assert!(yard.contains_in_generation(2, weak));
    }

    #[test]
    fn compact_recycles_dumped_generations_without_a_separate_reclaim() {
        let (_dir, paths, mut yard) = yard_with_paths(
            2,
            YardConfig {
                weak_budget: 0,
                strong_level_budget: 0,
                fanout: 1,
            },
        );
        // A strong blob lands in gen 0 and, with a zero budget, overflows on
        // compaction — the whole of gen 0 dumps into gen 1.
        let strong = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'S'; 512]))
            .unwrap();
        yard.pin_strong(pin_id(7), strong);
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

        yard.pin_strong(pin, old);
        yard.collect().unwrap();
        assert_eq!(
            get_raw(&yard.reader().unwrap(), old).unwrap(),
            raw_blob(b"old")
        );

        let new = yard.put::<RawBytes, _>(raw_blob(b"new")).unwrap();
        yard.pin_strong(pin, new);
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
                weak_budget: 0,
                ..YardConfig::default()
            },
        );
        let live = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'L'; 512]))
            .unwrap();
        let evicted = yard
            .put::<RawBytes, _>(Bytes::from_source(vec![b'E'; 4096]))
            .unwrap();

        yard.pin_strong(pin_id(6), live);
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

    /// The amnesia regression: weak pins are durable pile records, so
    /// reopening a yard rebuilds the weak state instead of resetting it.
    #[test]
    fn yard_open_reloads_weak_pins() {
        let (_dir, paths, mut yard) = yard_with_paths(2, YardConfig::default());

        // A pure want: pinned while absent, never fetched.
        let want =
            Blob::<RawBytes>::new(raw_blob(b"still wanted after restart")).get_handle();
        yard.pin_weak(want).unwrap();
        // A demand-fetched cache entry: pinned while absent, then put.
        let cached = Blob::<RawBytes>::new(raw_blob(b"cached")).get_handle();
        yard.pin_weak(cached).unwrap();
        yard.put::<RawBytes, _>(raw_blob(b"cached")).unwrap();
        // A retracted want must stay retracted across restart (LWW).
        let retracted = Blob::<RawBytes>::new(raw_blob(b"changed my mind")).get_handle();
        yard.pin_weak(retracted).unwrap();
        yard.unpin_weak(retracted).unwrap();

        drop(yard); // closes (and flushes) the generation piles

        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        let pinned: BTreeSet<_> = reopened
            .weak_pins()
            .unwrap()
            .map(|r| r.unwrap().raw)
            .collect();
        assert!(
            pinned.contains(&want.raw),
            "weak want lost across restart — the amnesia bug"
        );
        assert!(
            pinned.contains(&cached.raw),
            "weak cache-retention marker lost across restart"
        );
        assert!(
            !pinned.contains(&retracted.raw),
            "weak unpin did not stick across restart"
        );

        // The reloaded weak pin still works as a retention marker: the
        // cached blob survives collection under the default budget.
        reopened.collect().unwrap();
        let reader = reopened.reader().unwrap();
        assert_eq!(get_raw(&reader, cached).unwrap(), raw_blob(b"cached"));
    }

    /// A young-pile rewrite (reclaim) must not drop the durable weak set:
    /// surviving pins are re-recorded into the rewritten pile.
    #[test]
    fn weak_markers_survive_reclaim() {
        let (_dir, paths, mut yard) = yard_with_paths(1, YardConfig::default());

        let want = Blob::<RawBytes>::new(raw_blob(b"wanted, absent")).get_handle();
        yard.pin_weak(want).unwrap();
        let cached = Blob::<RawBytes>::new(raw_blob(b"cached blob")).get_handle();
        yard.pin_weak(cached).unwrap();
        yard.put::<RawBytes, _>(raw_blob(b"cached blob")).unwrap();

        // Rewrite the young pile: only live blobs are transferred, so the
        // marker records are dropped — and must be re-recorded.
        yard.reclaim().unwrap();

        drop(yard);
        let mut reopened = Yard::open(paths, YardConfig::default()).unwrap();
        let pinned: BTreeSet<_> = reopened
            .weak_pins()
            .unwrap()
            .map(|r| r.unwrap().raw)
            .collect();
        assert!(
            pinned.contains(&want.raw),
            "want marker lost by reclaim rewrite"
        );
        assert!(
            pinned.contains(&cached.raw),
            "cache marker lost by reclaim rewrite"
        );
        let reader = reopened.reader().unwrap();
        assert_eq!(get_raw(&reader, cached).unwrap(), raw_blob(b"cached blob"));
    }

    /// The fail-loud posture: opening a yard whose generation pile has a
    /// corrupt tail must surface the corruption (naming the file) WITHOUT
    /// truncating anything; `Yard::restore` is the explicit opt-in repair.
    #[test]
    fn open_fails_loud_on_corrupt_generation_without_truncating() {
        use std::io::Write;

        let (_dir, paths, mut yard) = yard_with_paths(1, YardConfig::default());
        let live = yard.put::<RawBytes, _>(raw_blob(b"survivor")).unwrap();
        drop(yard); // closes (and flushes) the generation pile

        // Corrupt the tail: append garbage that is not a valid record.
        {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&paths[0])
                .unwrap();
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

        // Explicit repair: restore truncates the invalid tail and the
        // valid prefix stays readable.
        let mut repaired = Yard::restore(paths.clone(), YardConfig::default()).unwrap();
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
        }

        impl Model {
            fn new() -> Self {
                Self {
                    handles: Vec::new(),
                    bytes: BTreeMap::new(),
                    absent: Vec::new(),
                }
            }
        }

        #[derive(Clone, Debug, PartialEq, Eq)]
        struct FinalState {
            live_by_generation: Vec<Vec<RawHandle>>,
            readable: Vec<RawHandle>,
        }

        #[derive(Clone, Copy, Debug)]
        enum WeakPinMode {
            YoungOnly,
            AnyKnownHandle,
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

        fn weak_pins(yard: &Yard) -> BTreeMap<RawHandle, u64> {
            let weak_state = yard.weak_state.lock().expect("weak pin mutex poisoned");
            weak_state
                .pins
                .clone()
                .into_iter()
                .map(|raw| {
                    let pin = weak_state
                        .pins
                        .get(&raw)
                        .expect("weak pin key resolves")
                        .last_used;
                    (raw, pin)
                })
                .collect()
        }

        fn strong_roots(yard: &Yard) -> Vec<RawHandle> {
            (&yard.strong_pins)
                .into_iter()
                .filter_map(|pin| yard.strong_pins.get(pin).copied())
                .map(|handle| handle.raw)
                .collect()
        }

        fn budgeted_weak(
            weak: &BTreeMap<RawHandle, u64>,
            present: &BTreeSet<RawHandle>,
            budget: usize,
        ) -> BTreeSet<RawHandle> {
            let mut candidates = weak
                .iter()
                .filter(|(raw, _)| present.contains(*raw))
                .map(|(raw, last_used)| (*raw, *last_used))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(_, last_used)| Reverse(*last_used));
            candidates
                .into_iter()
                .take(budget)
                .map(|(raw, _)| raw)
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
            weak: &BTreeSet<RawHandle>,
            model: &Model,
        ) -> BTreeSet<RawHandle> {
            let mut queue = VecDeque::new();
            for root in roots {
                if !weak.contains(root) {
                    queue.push_back(*root);
                }
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
                    if !weak.contains(&child)
                        && present.contains(&child)
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
            let weak_with_lru = weak_pins(yard);
            let weak = weak_with_lru.keys().copied().collect::<BTreeSet<_>>();
            let strong_keep = model_strong_keep(&strong_roots(yard), &present, &weak, model);
            let weak_keep = budgeted_weak(&weak_with_lru, &present, yard.config.weak_budget);

            present
                .into_iter()
                .filter(|raw| strong_keep.contains(raw) || weak_keep.contains(raw))
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
            let reader = yard.reader().unwrap();
            let live = live_union(yard);
            let weak = weak_pins(yard).keys().copied().collect::<BTreeSet<_>>();
            let strong_keep = model_strong_keep(&strong_roots(yard), &live, &weak, model);

            for raw in strong_keep.intersection(&live) {
                let expected = model
                    .bytes
                    .get(raw)
                    .unwrap_or_else(|| panic!("seed {seed} step {step}: unknown live handle"));
                assert_readable_bytes(&reader, *raw, expected, seed, step);
            }

            for raw in weak.intersection(&strong_keep) {
                panic!("seed {seed} step {step}: weak pin {raw:02X?} leaked into strong keep");
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

        fn choose_weak_target(
            yard: &Yard,
            rng: &mut SplitMix64,
            model: &mut Model,
            mode: WeakPinMode,
        ) -> RawHandle {
            match mode {
                WeakPinMode::AnyKnownHandle => choose_known_or_absent(rng, model),
                WeakPinMode::YoungOnly => {
                    let young = live_sets(yard)
                        .first()
                        .into_iter()
                        .flat_map(|set| set.iter())
                        .copied()
                        .collect::<Vec<_>>();
                    if !young.is_empty() && rng.chance(3, 4) {
                        young[rng.index(young.len())]
                    } else {
                        fresh_absent_handle(rng, model)
                    }
                }
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

        fn run_one(seed: u64, weak_pin_mode: WeakPinMode) -> FinalState {
            let (_dir, mut yard) = yard_with(
                GENERATIONS,
                YardConfig {
                    weak_budget: 3,
                    strong_level_budget: 2,
                    fanout: 2,
                },
            );
            let mut rng = SplitMix64::new(seed);
            let mut model = Model::new();

            for step in 0..STEPS {
                match rng.index(9) {
                    0 | 1 => put_fresh_blob(&mut yard, &mut model, &mut rng, seed, step),
                    2 => {
                        if !model.handles.is_empty() {
                            let pin = pin_id(rng.index(PIN_COUNT));
                            let raw = model.handles[rng.index(model.handles.len())];
                            yard.pin_strong(pin, unknown(raw));
                        }
                    }
                    3 => yard.unpin_strong(pin_id(rng.index(PIN_COUNT))),
                    4 => {
                        let raw = choose_weak_target(&yard, &mut rng, &mut model, weak_pin_mode);
                        yard.pin_weak(unknown(raw)).unwrap();
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
                run_one(0xC0DE_0000_0000_0000 ^ seed, WeakPinMode::YoungOnly);
            }
        }

        #[test]
        fn seeded_yard_property_sequences_are_deterministic() {
            for seed in [0, 13, 49] {
                let seed = 0xD57D_0000_0000_0000 ^ seed;
                assert_eq!(
                    run_one(seed, WeakPinMode::YoungOnly),
                    run_one(seed, WeakPinMode::YoungOnly),
                    "seed {seed} diverged"
                );
            }
        }

        #[test]
        fn seeded_yard_property_sequences_resident_weak_pins_are_noops() {
            // Full operation space, including attempts to weak-pin resident
            // (even already-tenured) blobs. With the resident-weak-pin
            // no-op, those attempts are inert — the want only ever targets
            // absent content — so "weak never tenures" and the live=keep
            // invariants all hold. (Seed 0xC0DE^2, step 32, exposed the
            // pre-no-op bug; it now passes alongside the rest of the sweep.)
            for seed in [0, 2, 7, 13, 31, 49] {
                run_one(0xC0DE_0000_0000_0000 ^ seed, WeakPinMode::AnyKnownHandle);
            }
        }

        #[test]
        fn weak_pin_on_resident_tenured_blob_is_a_noop() {
            let (_dir, mut yard) = yard_with(
                3,
                YardConfig {
                    weak_budget: 1,
                    strong_level_budget: 0,
                    fanout: 1,
                },
            );
            let tenured = yard
                .put::<UnknownBlob, _>(Bytes::from_source(b"tenured then weak".to_vec()))
                .unwrap();

            yard.pin_strong(pin_id(0), tenured);
            yard.compact().unwrap();
            assert!(yard.contains_in_generation(2, tenured));

            // Weak-pinning a blob you already hold is a no-op: the want
            // signal only ever targets absent content. The resident blob
            // stays strong in the bottom generation, never acquiring a weak
            // tag — which is exactly why "weak never tenures" holds by
            // construction rather than needing eviction machinery here.
            yard.pin_weak(tenured).unwrap();
            yard.compact().unwrap();

            assert!(
                yard.contains_in_generation(2, tenured),
                "resident weak-pin must be a no-op; the strong blob stays held"
            );
        }
    }
}
