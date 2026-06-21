//! Generational collection of piles for lazy-retention blob storage.
//!
//! A [`Yard`] keeps an ordered young-to-old sequence of [`Pile`](super::pile::Pile)
//! generations. Writes land in the youngest generation, reads search the union
//! of each generation's live PATCH set, and retention/compaction update those
//! PATCH sets without changing Pile's append-only storage contract.

use std::cmp::Reverse;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::path::Path;
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
    pub fn pin_weak<S>(&self, handle: Inline<Handle<S>>)
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
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

            let handles: Vec<_> = strong_here
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

            for raw in strong_here {
                self.generations[level].live.remove(&raw);
            }
        }

        self.collect()
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
        let unknown: Inline<Handle<UnknownBlob>> = handle.transmute();
        for generation in &self.generations {
            if generation.live.get(&unknown.raw).is_none() {
                continue;
            }

            match generation.reader.get::<T, S>(handle) {
                Ok(value) => return Ok(value),
                Err(GetBlobError::BlobNotFound) => continue,
                Err(err) => return Err(YardGetError::Pile(err)),
            }
        }

        self.weak_state
            .lock()
            .expect("weak pin mutex poisoned")
            .pin(unknown);
        Err(YardGetError::NotFound)
    }
}

impl BlobChildren for YardReader {
    fn children(&self, handle: Inline<Handle<UnknownBlob>>) -> Vec<Inline<Handle<UnknownBlob>>> {
        let Ok(blob) = self.get::<Blob<UnknownBlob>, UnknownBlob>(handle) else {
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
            if self.get::<Bytes, UnknownBlob>(candidate).is_ok() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::encodings::rawbytes::RawBytes;

    fn yard_with(generations: usize, config: YardConfig) -> (tempfile::TempDir, Yard) {
        let dir = tempfile::tempdir().unwrap();
        let paths = (0..generations)
            .map(|i| dir.path().join(format!("gen-{i}.pile")))
            .collect::<Vec<_>>();
        let yard = Yard::create(paths, config).unwrap();
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
        let weak = yard.put::<RawBytes, _>(raw_blob(b"weak")).unwrap();

        yard.pin_strong(pin_id(1), strong);
        yard.pin_weak(weak);
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
        let child = yard
            .put::<UnknownBlob, _>(Bytes::from_source(b"child".to_vec()))
            .unwrap();
        let parent = yard
            .put::<UnknownBlob, _>(Bytes::from_source(child.raw.to_vec()))
            .unwrap();

        yard.pin_strong(pin_id(2), parent);
        yard.pin_weak(child);
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
    fn lsm_compaction_tenures_strong_and_keeps_weak_young() {
        let (_dir, mut yard) = yard_with(
            3,
            YardConfig {
                weak_budget: 10,
                strong_level_budget: 0,
                fanout: 1,
            },
        );
        let strong = yard.put::<RawBytes, _>(raw_blob(b"tenured")).unwrap();
        let weak = yard.put::<RawBytes, _>(raw_blob(b"cache")).unwrap();
        yard.pin_strong(pin_id(4), strong);
        yard.pin_weak(weak);

        yard.compact().unwrap();

        assert!(!yard.contains_in_generation(0, strong));
        assert!(!yard.contains_in_generation(1, strong));
        assert!(yard.contains_in_generation(2, strong));
        assert!(yard.contains_in_generation(0, weak));
        assert!(!yard.contains_in_generation(1, weak));
        assert!(!yard.contains_in_generation(2, weak));
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
}
