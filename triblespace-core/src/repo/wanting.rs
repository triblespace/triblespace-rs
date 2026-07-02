//! `Wanting<S>`: the **no-network-by-construction** lazy reader.
//!
//! A [`Wanting`] wraps a store the same way a `triblespace-net` `Peer`
//! does (shared behind `Arc<Mutex<S>>` so a `&self` read can record
//! state), but where the Peer answers a read miss with a swarm fetch,
//! `Wanting` answers it with a **durable want**: it weak-pins the missing
//! handle, flushes the store so the marker survives an immediate process
//! exit, and returns [`WantGetError::NotYet`]. It never blocks on I/O it
//! doesn't own, and it never networks — this module lives in
//! `triblespace-core`, which has no network dependency at all, so the
//! guarantee is enforced by the linker, not by discipline.
//!
//! The intended division of labor:
//!
//! - A short-lived process (a faculty invocation) opens the shared pile
//!   as a [`LazyPile`] and reads. Every miss leaves a crash-durable
//!   weak-pin want on record (weak pins ARE the want-queue — see
//!   [`WeakPinStore`]) and comes back `NotYet`.
//! - A long-running daemon (`Peer` + `Reconciler` in `triblespace-net`,
//!   or `trible pile net sync --lazy`) enumerates the weak pins, fetches
//!   the absent blobs from the swarm, and lands them in the same pile.
//! - The next read — or a [`Wanting::wait_for`] poll loop — finds the
//!   bytes locally.
//!
//! `NotYet` therefore means "the want is durably recorded; retry later".
//! Absence is always "not obtained yet", never "definitely absent" —
//! existence is semidecidable, same as everywhere else in the lazy-sync
//! substrate.
//!
//! Failure posture is loud: if the want cannot be recorded (pin or flush
//! fails), the read returns [`WantGetError::WantRecord`] — it never
//! silently proceeds, because a silently-dropped want is a blob nobody
//! will ever fetch. Likewise [`Wanting::wait_for`] propagates a store
//! refresh error ([`WaitError::Store`]) immediately and never attempts
//! auto-repair.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anybytes::Bytes;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding};

use super::{
    BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult, StorageFlush,
    WeakPinStore,
};

/// A [`Wanting`] over the default on-disk store: the faculty-side view
/// of a shared [`Pile`](super::pile::Pile) — local reads, durable wants,
/// zero network.
pub type LazyPile = Wanting<super::pile::Pile>;

/// The want-record failure of a store `S`: either the weak pin itself or
/// the flush that makes it crash-durable failed.
pub type WantRecordErrorOf<S> =
    WantRecordError<<S as WeakPinStore>::WeakPinError, <S as StorageFlush>::Error>;

/// Recording a durable want failed. Both halves matter: a want that is
/// pinned but not flushed evaporates if the process exits before the
/// next sync — and a faculty exits right after its read.
#[derive(Debug)]
pub enum WantRecordError<P, F> {
    /// The weak pin could not be recorded.
    Pin(P),
    /// The weak pin was recorded but flushing it durably failed.
    Flush(F),
}

impl<P: std::error::Error, F: std::error::Error> std::fmt::Display for WantRecordError<P, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pin(e) => write!(f, "recording weak-pin want failed: {e}"),
            Self::Flush(e) => write!(f, "flushing weak-pin want failed: {e}"),
        }
    }
}

impl<P, F> std::error::Error for WantRecordError<P, F>
where
    P: std::error::Error + 'static,
    F: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pin(e) => Some(e),
            Self::Flush(e) => Some(e),
        }
    }
}

/// Error from a [`WantingReader`] get.
#[derive(Debug)]
pub enum WantGetError<E, W> {
    /// The bytes were present locally but didn't convert to the
    /// requested type.
    Conversion(E),
    /// Local miss. The want is **durably on record** (weak pin, flushed)
    /// — a sync daemon (`Peer` + `Reconciler`) services it; retry later.
    /// Never "definitely absent".
    NotYet,
    /// Local miss AND the want could not be durably recorded. The demand
    /// is NOT on record — the caller must not assume anyone will fetch
    /// this blob.
    WantRecord(W),
}

impl<E: std::error::Error, W: std::error::Error> std::fmt::Display for WantGetError<E, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conversion(e) => write!(f, "blob conversion failed: {e}"),
            Self::NotYet => write!(
                f,
                "blob not obtained yet (want durably recorded; retry after a sync daemon services it)"
            ),
            Self::WantRecord(e) => write!(f, "blob missing and want not recorded: {e}"),
        }
    }
}

impl<E, W> std::error::Error for WantGetError<E, W>
where
    E: std::error::Error + 'static,
    W: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion(e) => Some(e),
            Self::NotYet => None,
            Self::WantRecord(e) => Some(e),
        }
    }
}

/// Error from [`Wanting::wait_for`].
#[derive(Debug)]
pub enum WaitError<R, W> {
    /// The deadline elapsed without the blob appearing. The want stays
    /// durably on record — a later wait (or read) can still succeed.
    Deadline,
    /// The store failed to refresh (e.g. a corrupt pile tail). Propagated
    /// immediately — fail loud, never auto-restore.
    Store(R),
    /// The want could not be durably recorded (see [`WantRecordError`]).
    WantRecord(W),
}

impl<R: std::error::Error, W: std::error::Error> std::fmt::Display for WaitError<R, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deadline => write!(f, "deadline elapsed before the blob appeared (want stays recorded)"),
            Self::Store(e) => write!(f, "store refresh failed: {e}"),
            Self::WantRecord(e) => write!(f, "want not recorded: {e}"),
        }
    }
}

impl<R, W> std::error::Error for WaitError<R, W>
where
    R: std::error::Error + 'static,
    W: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Deadline => None,
            Self::Store(e) => Some(e),
            Self::WantRecord(e) => Some(e),
        }
    }
}

/// A store wrapped in want-recording lazy reads. See the [module-level
/// docs](self) for the full mental model.
///
/// Mirrors the shared-store shape of `triblespace-net`'s `Peer`
/// (`Arc<Mutex<S>>`) so a `&self` read on a [`WantingReader`] can record
/// the weak-pin want — the one piece of state a read must mutate.
pub struct Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    store: Arc<Mutex<S>>,
}

impl<S> Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    /// Wrap a store. No thread is spawned, no network is opened —
    /// `Wanting` is pure local mechanics.
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    /// Lock and borrow the underlying store, for store-specific methods
    /// that aren't part of the storage traits. Don't hold the guard
    /// across calls back into the `Wanting` — its own methods take the
    /// same lock.
    pub fn store(&self) -> MutexGuard<'_, S> {
        self.store.lock().expect("store mutex")
    }

    /// Consume the `Wanting` and return the underlying store.
    ///
    /// # Panics
    ///
    /// Panics if an outstanding [`WantingReader`] still shares the store
    /// — drop all readers first.
    pub fn into_store(self) -> S {
        match Arc::try_unwrap(self.store) {
            Ok(mutex) => mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Err(_) => panic!(
                "Wanting::into_store: an outstanding WantingReader still shares the store; drop readers first"
            ),
        }
    }

    /// **Blocking** poll loop: wait until `handle`'s bytes appear in the
    /// store (landed by a sync daemon servicing the want, or by any
    /// other writer), or until `deadline` elapses.
    ///
    /// Each iteration takes a fresh reader — which refreshes the store,
    /// so records appended by other processes become visible — and does
    /// a local get. The first miss records the durable want (pin + flush,
    /// exactly like the reader path), so `wait_for` is self-sufficient:
    /// calling it without a prior `get` still enqueues the demand.
    ///
    /// A store refresh error (e.g. a corrupt pile tail) is propagated
    /// immediately as [`WaitError::Store`] — fail loud, never
    /// auto-restore. On [`WaitError::Deadline`] the want stays durably
    /// recorded; retry later.
    pub fn wait_for(
        &mut self,
        handle: Inline<Handle<UnknownBlob>>,
        deadline: Duration,
        poll_every: Duration,
    ) -> Result<Bytes, WaitError<S::ReaderError, WantRecordErrorOf<S>>> {
        let start = Instant::now();
        loop {
            let reader = self.reader().map_err(WaitError::Store)?;
            match BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle) {
                Ok(bytes) => return Ok(bytes),
                Err(WantGetError::NotYet) => {}
                Err(WantGetError::WantRecord(e)) => return Err(WaitError::WantRecord(e)),
                // Bytes-from-UnknownBlob conversion is infallible.
                Err(WantGetError::Conversion(never)) => match never {},
            }
            if start.elapsed() >= deadline {
                return Err(WaitError::Deadline);
            }
            std::thread::sleep(poll_every);
        }
    }
}

impl<S> BlobStorePut for Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        self.store.lock().expect("store mutex").put(item)
    }
}

impl<S> BlobStore for Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Reader = WantingReader<S>;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let local = self.store.lock().expect("store mutex").reader()?;
        Ok(WantingReader {
            local,
            store: self.store.clone(),
        })
    }
}

impl<S> PinStore for Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type PinsError = S::PinsError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;
    // Collected eagerly: the inner store's iterator would borrow the
    // mutex guard, which cannot leave this call.
    type ListIter<'a> = std::vec::IntoIter<Result<Id, S::PinsError>> where S: 'a;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        let mut store = self.store.lock().expect("store mutex");
        let ids: Vec<Result<Id, S::PinsError>> = store.pins()?.collect();
        Ok(ids.into_iter())
    }

    fn head(
        &mut self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        self.store.lock().expect("store mutex").head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        self.store.lock().expect("store mutex").update(id, old, new)
    }
}

impl<S> WeakPinStore for Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type WeakPinError = S::WeakPinError;
    // Collected eagerly, same rationale as `pins`.
    type WeakListIter<'a> =
        std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, S::WeakPinError>> where S: 'a;

    /// Passthrough to the inner store. Note the durability contract of
    /// the *read* path (pin + flush) does not apply here — this is the
    /// store's own `pin_weak` semantics; call
    /// [`StorageFlush::flush`] yourself if you need the marker
    /// crash-durable immediately.
    fn pin_weak<Sch>(&mut self, handle: Inline<Handle<Sch>>) -> Result<(), Self::WeakPinError>
    where
        Sch: BlobEncoding + 'static,
        Handle<Sch>: InlineEncoding,
    {
        self.store.lock().expect("store mutex").pin_weak(handle)
    }

    /// Passthrough: retract a want / retention marker.
    fn unpin_weak<Sch>(&mut self, handle: Inline<Handle<Sch>>) -> Result<(), Self::WeakPinError>
    where
        Sch: BlobEncoding + 'static,
        Handle<Sch>: InlineEncoding,
    {
        self.store.lock().expect("store mutex").unpin_weak(handle)
    }

    fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError> {
        let mut store = self.store.lock().expect("store mutex");
        let pins: Vec<Result<Inline<Handle<UnknownBlob>>, S::WeakPinError>> =
            store.weak_pins()?.collect();
        Ok(pins.into_iter())
    }
}

impl<S> StorageFlush for Wanting<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Error = <S as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.store.lock().expect("store mutex").flush()
    }
}

/// The read view of a [`Wanting`]: the store's own reader snapshot plus
/// a want-recording handle into the shared store.
///
/// The sync [`BlobStoreGet`] is *local-plus-want*: a hit is served from
/// the snapshot; a miss durably records the demand (weak pin + flush)
/// and returns [`WantGetError::NotYet`]. It never blocks and never
/// networks.
///
/// Note that **every** miss records a want — including speculative
/// probes. Don't drive conservative reference scans ([`super::BlobChildren`])
/// through this reader; that's why it deliberately doesn't implement the
/// trait.
pub struct WantingReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    local: S::Reader,
    /// Want-recording handle into the shared store: a `&self` read must
    /// be able to weak-pin the missed handle and flush the marker.
    store: Arc<Mutex<S>>,
}

// Identity ignores the store handle: two readers are equal iff their
// local snapshots are — the handle is a capability, not part of the
// snapshot's value. (Mirrors `PeerReader` in triblespace-net.)
impl<S> Clone for WantingReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            store: self.store.clone(),
        }
    }
}
impl<S> PartialEq for WantingReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.local == other.local
    }
}
impl<S> Eq for WantingReader<S> where S: BlobStore + WeakPinStore + StorageFlush + Send + 'static {}

impl<S> BlobStoreGet for WantingReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type GetError<E: std::error::Error + Send + Sync + 'static> =
        WantGetError<E, WantRecordErrorOf<S>>;

    fn get<T, Sch>(
        &self,
        handle: Inline<Handle<Sch>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<Sch>>::Error>>
    where
        Sch: BlobEncoding + 'static,
        T: TryFromBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // Universal byte read against the local snapshot: bytes-by-hash,
        // so any store-level failure (not found, validation) is a miss
        // and the typed conversion happens exactly once, below.
        match self
            .local
            .get::<Bytes, UnknownBlob>(Inline::new(handle.raw))
        {
            Ok(bytes) => Blob::<Sch>::new(bytes)
                .try_from_blob()
                .map_err(WantGetError::Conversion),
            Err(_) => {
                // The durable want: weak-pin the demand, then flush —
                // the marker must survive an immediate process exit
                // (pile records are not durable until flushed). Any
                // failure here is an ERROR to the caller: a silently
                // dropped want is a blob nobody will ever fetch.
                let mut store = self.store.lock().expect("store mutex");
                store
                    .pin_weak(handle)
                    .map_err(|e| WantGetError::WantRecord(WantRecordError::Pin(e)))?;
                store
                    .flush()
                    .map_err(|e| WantGetError::WantRecord(WantRecordError::Flush(e)))?;
                Err(WantGetError::NotYet)
            }
        }
    }
}

impl<S> BlobStoreList for WantingReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Iter<'a> = <S::Reader as BlobStoreList>::Iter<'a> where Self: 'a;
    type Err = <S::Reader as BlobStoreList>::Err;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        self.local.blobs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::pile::Pile;

    fn blob_of(bytes: &'static [u8]) -> (Blob<UnknownBlob>, Inline<Handle<UnknownBlob>>) {
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(bytes));
        let handle = blob.get_handle();
        (blob, handle)
    }

    fn fresh_pile(path: &std::path::Path) -> Pile {
        std::fs::File::create(path).unwrap();
        Pile::open(path).unwrap()
    }

    /// A local hit serves from the snapshot: no want is recorded.
    #[test]
    fn local_hit_serves_without_recording_want() {
        let mut lazy = Wanting::new(MemoryRepo::default());
        let (blob, handle) = blob_of(b"resident");
        lazy.put::<UnknownBlob, _>(blob).unwrap();

        let reader = lazy.reader().unwrap();
        let bytes: Bytes = reader.get(handle).expect("resident blob serves locally");
        assert_eq!(&bytes[..], b"resident");
        assert_eq!(lazy.weak_pins().unwrap().count(), 0, "a hit records no want");
    }

    /// The core contract: a miss returns `NotYet` with the want durably
    /// on record — visible through the same handle AND through a second
    /// `Pile` opened fresh on the same file (the reader path flushed).
    #[test]
    fn miss_records_durable_want_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy.pile");
        let mut lazy: LazyPile = Wanting::new(fresh_pile(&path));
        let (_, handle) = blob_of(b"wanted but absent");

        let reader = lazy.reader().unwrap();
        let err = BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle)
            .expect_err("absent blob must not serve");
        assert!(matches!(err, WantGetError::NotYet), "miss is NotYet, got {err:?}");
        drop(reader);

        let wants: Vec<_> = lazy.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(wants, vec![handle], "want visible in weak_pins()");

        // A second handle opened fresh on the same file replays the
        // (flushed) marker — this is what a sync daemon's pile handle
        // sees after the faculty process exits.
        let mut reopened = Pile::open(&path).unwrap();
        let wants: Vec<_> = reopened.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(wants, vec![handle], "want survives reopen");
        reopened.close().unwrap();

        lazy.into_store().close().unwrap();
    }

    // ── WantRecord path: a store whose pin_weak fails ────────────────

    #[derive(Debug, PartialEq)]
    struct PinRefused;
    impl std::fmt::Display for PinRefused {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "weak pin refused")
        }
    }
    impl std::error::Error for PinRefused {}

    /// MemoryRepo wrapper whose weak-pin surface always fails —
    /// simulates a store that cannot record the want.
    #[derive(Debug, Default)]
    struct FailingPins(MemoryRepo);

    impl BlobStorePut for FailingPins {
        type PutError = <MemoryRepo as BlobStorePut>::PutError;
        fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
        where
            Sch: BlobEncoding + 'static,
            T: IntoBlob<Sch>,
            Handle<Sch>: InlineEncoding,
        {
            self.0.put(item)
        }
    }

    impl BlobStore for FailingPins {
        type Reader = <MemoryRepo as BlobStore>::Reader;
        type ReaderError = <MemoryRepo as BlobStore>::ReaderError;
        fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
            self.0.reader()
        }
    }

    impl PinStore for FailingPins {
        type PinsError = <MemoryRepo as PinStore>::PinsError;
        type HeadError = <MemoryRepo as PinStore>::HeadError;
        type UpdateError = <MemoryRepo as PinStore>::UpdateError;
        type ListIter<'a> = <MemoryRepo as PinStore>::ListIter<'a>;

        fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
            self.0.pins()
        }
        fn head(
            &mut self,
            id: Id,
        ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
            self.0.head(id)
        }
        fn update(
            &mut self,
            id: Id,
            old: Option<Inline<Handle<SimpleArchive>>>,
            new: Option<Inline<Handle<SimpleArchive>>>,
        ) -> Result<PushResult, Self::UpdateError> {
            self.0.update(id, old, new)
        }
    }

    impl WeakPinStore for FailingPins {
        type WeakPinError = PinRefused;
        type WeakListIter<'a> =
            std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, PinRefused>>;

        fn pin_weak<Sch>(&mut self, _handle: Inline<Handle<Sch>>) -> Result<(), PinRefused>
        where
            Sch: BlobEncoding + 'static,
            Handle<Sch>: InlineEncoding,
        {
            Err(PinRefused)
        }
        fn unpin_weak<Sch>(&mut self, _handle: Inline<Handle<Sch>>) -> Result<(), PinRefused>
        where
            Sch: BlobEncoding + 'static,
            Handle<Sch>: InlineEncoding,
        {
            Err(PinRefused)
        }
        fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, PinRefused> {
            Ok(Vec::new().into_iter())
        }
    }

    impl StorageFlush for FailingPins {
        type Error = std::convert::Infallible;
        fn flush(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// A failed want-record is an ERROR to the caller — never a silent
    /// `NotYet` (a silently dropped want is a blob nobody will fetch).
    /// The flush half of the failure ([`WantRecordError::Flush`]) shares
    /// this exact propagation path; only the pin half is simulated here
    /// because a failing-but-typed flush needs no separate mechanism.
    #[test]
    fn want_record_failure_is_an_error() {
        let mut lazy = Wanting::new(FailingPins::default());
        let (_, handle) = blob_of(b"unrecordable");

        let reader = lazy.reader().unwrap();
        let err = BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle)
            .expect_err("miss on a pin-refusing store must error");
        assert!(
            matches!(err, WantGetError::WantRecord(WantRecordError::Pin(PinRefused))),
            "want-record failure must surface as WantRecord, got {err:?}"
        );
    }

    // ── wait_for ─────────────────────────────────────────────────────

    /// `wait_for` returns the bytes once another handle lands the blob
    /// in the shared file — the faculty-blocks-while-daemon-fetches
    /// round trip, with a plain second `Pile` standing in for the daemon.
    #[test]
    fn wait_for_returns_blob_once_landed_by_second_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.pile");
        let mut lazy: LazyPile = Wanting::new(fresh_pile(&path));
        let (blob, handle) = blob_of(b"landed later");

        let writer_path = path.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            let mut pile = Pile::open(&writer_path).unwrap();
            pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.close().unwrap();
        });

        let bytes = lazy
            .wait_for(handle, Duration::from_secs(30), Duration::from_millis(10))
            .expect("blob appears within the deadline");
        assert_eq!(&bytes[..], b"landed later");
        writer.join().unwrap();

        lazy.into_store().close().unwrap();
    }

    /// Deadline expiry: `Deadline` comes back, and the want stays
    /// durably recorded (wait_for is self-sufficient — no prior get
    /// needed to enqueue the demand).
    #[test]
    fn wait_for_deadline_expires_and_want_stays_recorded() {
        let mut lazy = Wanting::new(MemoryRepo::default());
        let (_, handle) = blob_of(b"nobody lands this");

        let err = lazy
            .wait_for(handle, Duration::from_millis(50), Duration::from_millis(5))
            .expect_err("nobody lands the blob");
        assert!(matches!(err, WaitError::Deadline), "expected Deadline, got {err:?}");

        let wants: Vec<_> = lazy.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(wants, vec![handle], "wait_for recorded the want");
    }

    /// A corrupt pile tail fails loud and immediately (`Store(ReadError)`)
    /// — never auto-restored, never spun on until the deadline.
    #[test]
    fn wait_for_corrupt_tail_fails_loud() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.pile");
        let mut lazy: LazyPile = Wanting::new(fresh_pile(&path));
        let (_, handle) = blob_of(b"unreachable");

        // Corrupt the tail out-of-band: garbage that matches no record
        // magic.
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xAB; 256]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let started = Instant::now();
        let err = lazy
            .wait_for(handle, Duration::from_secs(30), Duration::from_millis(10))
            .expect_err("corrupt tail must fail");
        assert!(
            matches!(
                err,
                WaitError::Store(crate::repo::pile::ReadError::CorruptPile { .. })
            ),
            "expected Store(CorruptPile), got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "corruption must fail immediately, not spin to the deadline"
        );
    }
}
