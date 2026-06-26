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

use super::pile::{GetBlobError, InsertError, Pile, PileReader, ReadError};
use super::{
    reachable, transfer, BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut,
    StorageClose, TransferError,
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
struct Generation {
    path: PathBuf,
    pile: Option<Pile>,
    live: HandleSet,
}

impl Generation {
    fn pile_mut(&mut self) -> &mut Pile {
        self.pile
            .as_mut()
            .expect("yard generation pile already closed")
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
            let mut pile = Pile::open(&path).map_err(YardOpenError::Pile)?;
            pile.restore().map_err(YardOpenError::Pile)?;
            generations.push(Generation {
                path,
                pile: Some(pile),
                live: HandleSet::new(),
            });
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
    pub fn open<P>(
        paths: impl IntoIterator<Item = P>,
        config: YardConfig,
    ) -> Result<Self, YardOpenError>
    where
        P: AsRef<Path>,
    {
        let mut generations = Vec::new();
        for path in paths {
            let path = path.as_ref().to_path_buf();
            let mut pile = Pile::open(&path).map_err(YardOpenError::Pile)?;
            pile.restore().map_err(YardOpenError::Pile)?;
            let reader = pile.reader().map_err(YardOpenError::Pile)?;
            let live = collect_list(reader.blobs()).map_err(YardOpenError::List)?;
            generations.push(Generation {
                path,
                pile: Some(pile),
                live,
            });
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

    /// Number of generations in young-to-old order.
    pub fn generation_count(&self) -> usize {
        self.generations.len()
    }

    /// Number of live blobs in a generation.
    pub fn generation_len(&self, level: usize) -> Option<usize> {
        self.generations.get(level).map(|g| g.live.len() as usize)
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
            .is_some_and(|g| g.live.get(&handle.raw).is_some())
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

    /// Weakly pin a blob and refresh its LRU recency.
    ///
    /// A weak pin is the demand-born want-signal for content you *lack*
    /// (minted on a get-miss). Weak-pinning a blob you already hold
    /// (resident in any generation) is a **no-op**: weak entries originate
    /// only from demand, never from tagging resident content. (Releasing
    /// resident content you no longer need durably is a separate concern —
    /// the local→distributed handoff — not wired here yet.)
    pub fn pin_weak<S>(&self, handle: Inline<Handle<S>>)
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
        if self
            .generations
            .iter()
            .any(|generation| generation.live.get(&handle.raw).is_some())
        {
            return;
        }
        self.weak_state
            .lock()
            .expect("weak pin mutex poisoned")
            .pin(handle);
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
            generation.live = generation.live.intersect(&keep);
        }
        Ok(())
    }

    /// Run one compaction pass.
    ///
    /// Strong survivors descend when a level exceeds its strong budget. Weak
    /// pins are only retained in the young generation and never copied down.
    pub fn compact(&mut self) -> Result<(), YardCollectError> {
        self.collect()?;
        let reader = self.reader().map_err(YardCollectError::Reader)?;
        let strong_keep = self.strong_keep_set(&reader);
        let last = self.generations.len().saturating_sub(1);

        for level in 0..last {
            let strong_here = self.generations[level].live.intersect(&strong_keep);
            if strong_here.len() as usize <= self.strong_budget_for(level) {
                continue;
            }

            // Overflow: dump the whole level down — strong *and* weak
            // survivors. `collect()` above already dropped dead, so `live`
            // here is exactly the survivors. Weak is allowed to descend and
            // use space in lower tiers rather than being pinned to the
            // youngest generation; it stays evictable everywhere and is
            // dropped by the weak budget under pressure.
            let movers = self.generations[level].live.clone();
            let handles: Vec<_> = movers
                .clone()
                .into_iter()
                .map(Inline::<Handle<UnknownBlob>>::new)
                .collect();

            let mut copied = Vec::new();
            {
                let target = self.generations[level + 1].pile_mut();
                for result in transfer(&reader, target, handles.clone()) {
                    let (source, _target) = result.map_err(YardCollectError::Transfer)?;
                    copied.push(source);
                }
            }

            for source in copied {
                self.generations[level + 1]
                    .live
                    .insert(&Entry::new(&source.raw));
            }

            for raw in movers {
                self.generations[level].live.remove(&raw);
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
        for (level, generation) in self.generations.iter_mut().enumerate() {
            let path = generation.path.clone();
            let temp_path = reclaim_temp_path(&path, level);
            let live = generation.live.clone();
            let pile = generation
                .pile
                .take()
                .expect("yard generation pile already closed");

            match reclaim_generation(&path, &temp_path, &live, pile) {
                Ok(pile) => generation.pile = Some(pile),
                Err(err) => {
                    if let Ok(mut pile) = Pile::open(&path) {
                        if pile.restore().is_ok() {
                            generation.pile = Some(pile);
                        }
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
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
        let handle = self.generations[level].pile_mut().put::<S, T>(item)?;
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations[level]
            .live
            .insert(&Entry::new(&unknown.raw));
        Ok(handle)
    }
}

impl Drop for Yard {
    fn drop(&mut self) {
        for generation in &mut self.generations {
            if let Some(pile) = generation.pile.take() {
                let _ = pile.close();
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
        let handle = self.generations[0].pile_mut().put::<S, T>(item)?;
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        self.generations[0].live.insert(&Entry::new(&unknown.raw));
        Ok(handle)
    }
}

impl BlobStore for Yard {
    type Reader = YardReader;
    type ReaderError = YardReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let mut generations = Vec::with_capacity(self.generations.len());
        for generation in &mut self.generations {
            generations.push(YardGenerationReader {
                reader: generation
                    .pile_mut()
                    .reader()
                    .map_err(YardReaderError::Pile)?,
                live: generation.live.clone(),
            });
        }
        Ok(YardReader {
            generations,
            weak_state: self.weak_state.clone(),
        })
    }
}

impl StorageClose for Yard {
    type Error = YardCloseError;

    fn close(mut self) -> Result<(), Self::Error> {
        for generation in &mut self.generations {
            if let Some(pile) = generation.pile.take() {
                pile.close().map_err(YardCloseError::Pile)?;
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
    reopened.restore().map_err(YardReclaimError::Pile)?;
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
    Pile(ReadError),
    List(GetBlobError<Infallible>),
}

impl fmt::Display for YardOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoGenerations => write!(f, "yard requires at least one generation"),
            Self::Io(err) => write!(f, "failed to create yard pile file: {err}"),
            Self::Pile(err) => write!(f, "failed to open yard pile: {err}"),
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
}

impl fmt::Display for YardCollectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reader(err) => write!(f, "failed to create yard reader: {err}"),
            Self::Transfer(err) => write!(f, "failed to compact yard generation: {err}"),
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
}

impl fmt::Display for YardReclaimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "failed to replace yard generation pile: {err}"),
            Self::Pile(err) => write!(f, "failed to read yard generation pile: {err}"),
            Self::Transfer(err) => write!(f, "failed to copy live yard blobs: {err}"),
            Self::Close(err) => write!(f, "failed to close yard generation pile: {err}"),
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
        pile.restore().unwrap();
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
        yard.pin_weak(weak);
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
        yard.pin_weak(child);
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
        yard.pin_weak(absent);

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
        yard.pin_weak(weak);
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
        fresh_pile.restore().unwrap();
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
                .map(|generation| generation.live.clone().into_iter().collect())
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
                        yard.pin_weak(unknown(raw));
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
            yard.pin_weak(tenured);
            yard.compact().unwrap();

            assert!(
                yard.contains_in_generation(2, tenured),
                "resident weak-pin must be a no-op; the strong blob stays held"
            );
        }
    }
}
