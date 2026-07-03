//! `Lazy<S>`: the **no-network-by-construction** lazy reader. A
//! `Lazy<S>` records durable wants on miss — the type is the policy
//! (lazy), wants are the mechanism it records.
//!
//! A [`Lazy`] wraps a store the same way a `triblespace-net` `Peer`
//! does (shared behind `Arc<Mutex<S>>` so a `&self` read can record
//! state), but where the Peer answers a read miss with a swarm fetch,
//! `Lazy` answers it with a **durable want**: it weak-pins the missing
//! handle and flushes the store so the marker survives an immediate
//! process exit. It never performs I/O it doesn't own, and it never
//! networks — this module lives in `triblespace-core`, which has no
//! network dependency at all, so the guarantee is enforced by the
//! linker, not by discipline. Waiting for a blob is pure suspension:
//! an async read parks until the bytes land locally.
//!
//! Not to be confused with [`LazyLock`](std::sync::LazyLock) /
//! [`LazyCell`](std::cell::LazyCell): std's lazies *compute* their
//! value on first demand, so it always arrives. A `Lazy<S>` value is
//! *delivered by another party* (a sync daemon, another writer) — it
//! may never arrive, and the want persists durably either way.
//!
//! Like `PeerReader`, existence-vs-retrieval is split by *which trait
//! you call*, not by a bespoke method:
//!
//! - the **sync** [`BlobStoreGet`] on [`LazyReader`] is the instant
//!   probe: a hit serves from the snapshot; a miss durably records the
//!   want and returns [`WantGetError::NotYet`] immediately — it never
//!   waits.
//! - the **async** [`AsyncBlobStoreGet`] on [`LazyReader`] is the
//!   waiting read: a hit resolves immediately; a miss durably records
//!   the SAME want and then suspends until the blob appears in the
//!   store — landed by a sync daemon servicing the want, or by any
//!   other writer. Compose deadlines externally
//!   (`tokio::time::timeout`, a `select`); the future itself never
//!   gives up. Dropping it abandons the wait — the want STAYS on
//!   record.
//!
//! The intended division of labor:
//!
//! - A short-lived process (a faculty invocation) opens the shared pile
//!   as a `Lazy<Pile>` and reads. Every miss leaves a crash-durable
//!   weak-pin want on record (weak pins ARE the want-queue — see
//!   [`WeakPinStore`]) and comes back `NotYet` (sync probe) or suspends
//!   (async read).
//! - A long-running daemon (`Peer` + `Reconciler` in `triblespace-net`,
//!   or `trible pile net sync --lazy`) enumerates the weak pins, fetches
//!   the absent blobs from the swarm, and lands them in the same pile.
//! - The next sync probe — or the still-suspended async read — finds
//!   the bytes locally.
//!
//! *How* a suspended read wakes is an implementation detail, not API:
//! a `put` through the same `Lazy` signals waiters directly, and a
//! background cadence re-checks the store (with a refresh, so records
//! appended by other handles or processes become visible) while any
//! waiter is parked. No async runtime is required or assumed — the
//! future is executor-agnostic, waking through the standard
//! [`Waker`](std::task::Waker) contract.
//!
//! Absence is always "not obtained yet", never "definitely absent" —
//! existence is semidecidable, same as everywhere else in the lazy-sync
//! substrate.
//!
//! Failure posture is loud: if the want cannot be recorded (pin or flush
//! fails), the read returns a want-record error ([`WantGetError::WantRecord`]
//! on the probe, [`WantWaitError::WantRecord`] on the async read) — it
//! never silently proceeds, because a silently-dropped want is a blob
//! nobody will ever fetch. Likewise the async read propagates a store
//! refresh error ([`WantWaitError::Store`], e.g. a corrupt pile tail)
//! immediately and never attempts auto-repair.
//!
//! [`AsyncBlobStoreGet`]: crate::repo::async_store::AsyncBlobStoreGet
//! [`BlobStoreGet`]: crate::repo::BlobStoreGet
//! [`LazyReader`]: crate::repo::lazy::LazyReader
//! [`Lazy`]: crate::repo::lazy::Lazy
//! [`WeakPinStore`]: crate::repo::WeakPinStore
//! [`WantGetError::NotYet`]: crate::repo::lazy::WantGetError::NotYet
//! [`WantGetError::WantRecord`]: crate::repo::lazy::WantGetError::WantRecord
//! [`WantWaitError::Store`]: crate::repo::lazy::WantWaitError::Store
//! [`WantWaitError::WantRecord`]: crate::repo::lazy::WantWaitError::WantRecord

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use anybytes::Bytes;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding, RawInline};

use super::async_store::{
    AsyncBlobStore, AsyncBlobStoreGet, AsyncBlobStoreList, AsyncBlobStorePut,
};
use super::{
    BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult, StorageFlush,
    WeakPinStore,
};

/// Fixed cadence at which a suspended async read re-checks the store
/// for blobs landed by writers this `Lazy` cannot observe directly
/// (another handle to the same pile, another process appending to it).
/// Purely an implementation detail — in-process `put`s wake waiters
/// immediately, so this bounds only the *cross-process* wake latency.
const WANT_RECHECK_CADENCE: Duration = Duration::from_millis(100);

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

/// Error from a [`LazyReader`]'s **sync probe** ([`BlobStoreGet`]).
#[derive(Debug)]
pub enum WantGetError<E, W> {
    /// The bytes were present locally but didn't convert to the
    /// requested type.
    Conversion(E),
    /// Local miss. The want is **durably on record** (weak pin, flushed)
    /// — a sync daemon (`Peer` + `Reconciler`) services it. This is the
    /// probe's "recorded, not present" outcome, never "definitely
    /// absent"; to *wait* for the blob instead, use the async
    /// [`AsyncBlobStoreGet`]
    /// read, which suspends rather than erroring.
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
                "blob not obtained yet (want durably recorded; a sync daemon services it)"
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

/// Error from a [`LazyReader`]'s **async waiting read**
/// ([`AsyncBlobStoreGet`]).
///
/// There is deliberately no "not yet" variant: the async read *resolves*
/// when the blob lands instead of erroring on absence. Bound the wait
/// externally (`tokio::time::timeout`, a `select`) if you need a
/// deadline — on timeout the want stays durably recorded.
#[derive(Debug)]
pub enum WantWaitError<E, R, W> {
    /// The bytes landed but didn't convert to the requested type.
    Conversion(E),
    /// The store failed to refresh while re-checking (e.g. a corrupt
    /// pile tail). Propagated immediately — fail loud, never
    /// auto-restore.
    Store(R),
    /// The want could not be durably recorded (see [`WantRecordError`]).
    /// The read errors instead of suspending: a wait without a recorded
    /// want is a wait nobody will ever satisfy.
    WantRecord(W),
}

impl<E: std::error::Error, R: std::error::Error, W: std::error::Error> std::fmt::Display
    for WantWaitError<E, R, W>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conversion(e) => write!(f, "blob conversion failed: {e}"),
            Self::Store(e) => write!(f, "store refresh failed: {e}"),
            Self::WantRecord(e) => write!(f, "blob missing and want not recorded: {e}"),
        }
    }
}

impl<E, R, W> std::error::Error for WantWaitError<E, R, W>
where
    E: std::error::Error + 'static,
    R: std::error::Error + 'static,
    W: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion(e) => Some(e),
            Self::Store(e) => Some(e),
            Self::WantRecord(e) => Some(e),
        }
    }
}

/// The wake channel between a `Lazy`'s put-side and its suspended
/// async reads. Pure `std`: a parked-waker list drained by
/// [`wake_all`](Self::wake_all) plus a lazily-spawned cadence ticker
/// that covers landings from other handles/processes. No runtime, no
/// non-`std` dependency — waking rides the standard [`Waker`] contract,
/// so the futures work under any executor.
///
/// Lock order (where both are held): the store mutex first, then
/// `wakers`. Registering under the store lock is what makes the
/// in-process wake race-free: a `put` can only mutate the store *after*
/// a waiter's miss-check-plus-registration completes, so its `wake_all`
/// always sees the parked waker.
struct WantSignal {
    wakers: Mutex<Vec<Waker>>,
    /// Whether a cadence ticker thread is currently alive.
    ticker_alive: AtomicBool,
}

impl WantSignal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            wakers: Mutex::new(Vec::new()),
            ticker_alive: AtomicBool::new(false),
        })
    }

    /// Wake every parked waiter (drain-and-wake). Called by the
    /// put-side after landing a blob, and by the cadence ticker.
    fn wake_all(&self) {
        let drained: Vec<Waker> = std::mem::take(&mut *self.wakers.lock().expect("wakers mutex"));
        for waker in drained {
            waker.wake();
        }
    }

    /// Park `waker` until the next [`wake_all`](Self::wake_all).
    fn register(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock().expect("wakers mutex");
        if !wakers.iter().any(|w| w.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    /// Ensure the cadence ticker is running: a thread that wakes all
    /// parked waiters every [`WANT_RECHECK_CADENCE`], causing them to
    /// re-poll (which takes a fresh reader = a store refresh) and so
    /// observe blobs landed by writers this `Lazy` cannot see
    /// directly. Holds only a [`Weak`](std::sync::Weak) reference and
    /// retires itself when no waiter is parked, so it never outlives
    /// demand.
    fn ensure_ticker(this: &Arc<Self>) {
        if this.ticker_alive.swap(true, Ordering::AcqRel) {
            return; // already running
        }
        let weak = Arc::downgrade(this);
        std::thread::spawn(move || loop {
            std::thread::sleep(WANT_RECHECK_CADENCE);
            let Some(signal) = weak.upgrade() else { break };
            let drained: Vec<Waker> =
                std::mem::take(&mut *signal.wakers.lock().expect("wakers mutex"));
            if drained.is_empty() {
                // Nobody is parked: retire. Re-check for a registration
                // that raced the retirement and take the ticker back if
                // one slipped in (unless a fresh ticker already spawned).
                signal.ticker_alive.store(false, Ordering::Release);
                if signal.wakers.lock().expect("wakers mutex").is_empty()
                    || signal.ticker_alive.swap(true, Ordering::AcqRel)
                {
                    break;
                }
                continue;
            }
            for waker in drained {
                waker.wake();
            }
        });
    }
}

/// A store wrapped in want-recording lazy reads. See the [module-level
/// docs](self) for the full mental model.
///
/// Mirrors the shared-store shape of `triblespace-net`'s `Peer`
/// (`Arc<Mutex<S>>`) so a `&self` read on a [`LazyReader`] can record
/// the weak-pin want — the one piece of state a read must mutate.
pub struct Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    store: Arc<Mutex<S>>,
    signal: Arc<WantSignal>,
}

impl<S> Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    /// Wrap a store. No network is opened — `Lazy` is pure local
    /// mechanics. (A cadence re-check thread is spawned lazily only
    /// while an async read is suspended, and retires itself when none
    /// is.)
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            signal: WantSignal::new(),
        }
    }

    /// Lock and borrow the underlying store, for store-specific methods
    /// that aren't part of the storage traits. Don't hold the guard
    /// across calls back into the `Lazy` — its own methods take the
    /// same lock. Note that blobs landed through this raw guard bypass
    /// the immediate waiter wake-up; suspended async reads still observe
    /// them at the next cadence re-check.
    pub fn store(&self) -> MutexGuard<'_, S> {
        self.store.lock().expect("store mutex")
    }

    /// Consume the `Lazy` and return the underlying store.
    ///
    /// # Panics
    ///
    /// Panics if an outstanding [`LazyReader`] still shares the store
    /// — drop all readers first.
    pub fn into_store(self) -> S {
        match Arc::try_unwrap(self.store) {
            Ok(mutex) => mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Err(_) => panic!(
                "Lazy::into_store: an outstanding LazyReader still shares the store; drop readers first"
            ),
        }
    }
}

impl<S> BlobStorePut for Lazy<S>
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
        let result = self.store.lock().expect("store mutex").put(item);
        if result.is_ok() {
            // The landed blob may be exactly what a suspended async
            // read is waiting for.
            self.signal.wake_all();
        }
        result
    }
}

impl<S> BlobStore for Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Reader = LazyReader<S>;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let local = self.store.lock().expect("store mutex").reader()?;
        Ok(LazyReader {
            local,
            store: self.store.clone(),
            signal: self.signal.clone(),
        })
    }
}

impl<S> PinStore for Lazy<S>
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

impl<S> WeakPinStore for Lazy<S>
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

impl<S> StorageFlush for Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Error = <S as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.store.lock().expect("store mutex").flush()
    }
}

// ── Async surface ────────────────────────────────────────────────────

/// Async put: semantically identical to the sync [`BlobStorePut`] (a
/// local store write is genuinely synchronous — the future resolves on
/// first poll), present so a generic async consumer can drive a
/// `Lazy` end to end. Landing a blob wakes suspended async reads.
impl<S> AsyncBlobStorePut for Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type PutError = S::PutError;

    fn put<Sch, T>(
        &mut self,
        item: T,
    ) -> impl Future<Output = Result<Inline<Handle<Sch>>, Self::PutError>> + Send
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // Serialise before the future so it captures only `Send` values
        // (bytes + raw handle + shared handles) — never the
        // phantom-typed item (mirrors `SyncAsAsync::put`).
        let blob: Blob<Sch> = item.to_blob();
        let raw = blob.get_handle().raw;
        let bytes = blob.bytes;
        let store = self.store.clone();
        let signal = self.signal.clone();
        async move {
            let result = store
                .lock()
                .expect("store mutex")
                .put::<Sch, Blob<Sch>>(Blob::new(bytes));
            match result {
                Ok(_) => {
                    signal.wake_all();
                    Ok(Inline::new(raw))
                }
                Err(e) => Err(e),
            }
        }
    }
}

/// Async reader creation: resolves on first poll (taking a reader is a
/// local operation). The reader carries both read surfaces — the sync
/// probe and the async waiting read.
impl<S> AsyncBlobStore for Lazy<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
    S::Reader: Sync,
{
    type Reader = LazyReader<S>;
    type ReaderError = S::ReaderError;

    fn reader(
        &mut self,
    ) -> impl Future<Output = Result<Self::Reader, Self::ReaderError>> + Send {
        let store = self.store.clone();
        let signal = self.signal.clone();
        async move {
            let local = store.lock().expect("store mutex").reader()?;
            Ok(LazyReader {
                local,
                store,
                signal,
            })
        }
    }
}

/// The suspension behind the async waiting read. Every poll takes a
/// fresh reader from the live store — which refreshes it, so records
/// appended by other processes become visible — and does a local get.
/// The first miss records the durable want (pin + flush, exactly like
/// the sync probe), then the future parks until woken: by an in-process
/// `put` through the owning [`Lazy`], or by the cadence ticker.
///
/// Cancellation-safe: dropping the future abandons the wait; the
/// durable want remains recorded.
struct WaitForBlob<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    store: Arc<Mutex<S>>,
    signal: Arc<WantSignal>,
    raw: RawInline,
    want_recorded: bool,
}

impl<S> Future for WaitForBlob<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Output = Result<
        Bytes,
        WantWaitError<std::convert::Infallible, S::ReaderError, WantRecordErrorOf<S>>,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let handle = Inline::<Handle<UnknownBlob>>::new(this.raw);

        // Fresh reader = store refresh; a refresh failure (corrupt pile
        // tail) is loud and immediate, never spun on.
        let mut store = this.store.lock().expect("store mutex");
        let reader = match store.reader() {
            Ok(reader) => reader,
            Err(e) => return Poll::Ready(Err(WantWaitError::Store(e))),
        };
        // Universal byte read: any store-level failure is a miss.
        if let Ok(bytes) = BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle) {
            return Poll::Ready(Ok(bytes));
        }

        // The durable want, recorded once: weak-pin the demand, then
        // flush — the marker must survive an immediate process exit. Any
        // failure is an ERROR: a silently dropped want is a blob nobody
        // will ever fetch, and a wait without a recorded want is a wait
        // nobody will ever satisfy.
        if !this.want_recorded {
            if let Err(e) = store.pin_weak(handle) {
                return Poll::Ready(Err(WantWaitError::WantRecord(WantRecordError::Pin(e))));
            }
            if let Err(e) = store.flush() {
                return Poll::Ready(Err(WantWaitError::WantRecord(WantRecordError::Flush(e))));
            }
            this.want_recorded = true;
        }

        // Park — registered while the store lock is still held, so an
        // in-process `put` cannot land between our miss-check and our
        // registration (its store mutation serializes after this poll's
        // lock, and its wake_all after that).
        this.signal.register(cx.waker());
        drop(store);
        // Cover landings this `Lazy` can't observe directly (another
        // handle / another process): cadence re-check while parked.
        WantSignal::ensure_ticker(&this.signal);
        Poll::Pending
    }
}

/// The async **waiting read**: present resolves immediately; missing
/// records the durable want and suspends until the blob lands in the
/// store. See the [module-level docs](self) for the probe/wait split.
impl<S> AsyncBlobStoreGet for LazyReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type GetError<E: std::error::Error + Send + Sync + 'static> =
        WantWaitError<E, S::ReaderError, WantRecordErrorOf<S>>;

    fn get<T, Sch>(
        &self,
        handle: Inline<Handle<Sch>>,
    ) -> impl Future<Output = Result<T, Self::GetError<<T as TryFromBlob<Sch>>::Error>>> + Send
    where
        Sch: BlobEncoding + 'static,
        T: TryFromBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // Capture only `Send` values — the raw 32 bytes and the shared
        // store/signal handles, never the phantom-typed handle (mirrors
        // `SyncAsAsync` / `PeerReader`). The typed conversion happens at
        // completion, after the final await, so `T`/`Sch` are never part
        // of the future's held state.
        let wait = WaitForBlob {
            store: self.store.clone(),
            signal: self.signal.clone(),
            raw: handle.raw,
            want_recorded: false,
        };
        async move {
            match wait.await {
                Ok(bytes) => Blob::<Sch>::new(bytes)
                    .try_from_blob()
                    .map_err(WantWaitError::Conversion),
                Err(WantWaitError::Store(e)) => Err(WantWaitError::Store(e)),
                Err(WantWaitError::WantRecord(e)) => Err(WantWaitError::WantRecord(e)),
                // Bytes-from-UnknownBlob conversion is infallible.
                Err(WantWaitError::Conversion(never)) => match never {},
            }
        }
    }
}

/// Async listing over the local snapshot — zero-await, resolves on
/// first poll (enumeration is local; it never records wants).
impl<S> AsyncBlobStoreList for LazyReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
    S::Reader: Sync,
{
    type Err = <S::Reader as BlobStoreList>::Err;

    // Not an `async fn`: the desugared form would drop the explicit
    // `Send` bound the trait contract requires.
    #[allow(clippy::manual_async_fn)]
    fn blobs(
        &self,
    ) -> impl Future<Output = Vec<Result<Inline<Handle<UnknownBlob>>, Self::Err>>> + Send {
        async move { self.local.blobs().collect() }
    }
}

/// The read view of a [`Lazy`]: the store's own reader snapshot plus
/// a want-recording handle into the shared store.
///
/// Two read surfaces with deliberately different semantics (see the
/// [module-level docs](self)):
/// - the sync [`BlobStoreGet`] is the *probe*: local-plus-want — a hit
///   is served from the snapshot; a miss durably records the demand
///   (weak pin + flush) and returns [`WantGetError::NotYet`]
///   immediately.
/// - the async [`AsyncBlobStoreGet`]
///   is the *waiting read*: same durable want on miss, then suspension
///   until the blob lands (checked against the live store, not this
///   snapshot).
///
/// Note that **every** miss records a want — including speculative
/// probes. Don't drive conservative reference scans ([`super::BlobChildren`])
/// through this reader; that's why it deliberately doesn't implement the
/// trait.
pub struct LazyReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    local: S::Reader,
    /// Want-recording handle into the shared store: a `&self` read must
    /// be able to weak-pin the missed handle and flush the marker.
    store: Arc<Mutex<S>>,
    /// Wake channel shared with the owning [`Lazy`], so a suspended
    /// async read hears about in-process landings immediately.
    signal: Arc<WantSignal>,
}

// Identity ignores the store handle: two readers are equal iff their
// local snapshots are — the handle is a capability, not part of the
// snapshot's value. (Mirrors `PeerReader` in triblespace-net.)
impl<S> Clone for LazyReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            store: self.store.clone(),
            signal: self.signal.clone(),
        }
    }
}
impl<S> PartialEq for LazyReader<S>
where
    S: BlobStore + WeakPinStore + StorageFlush + Send + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.local == other.local
    }
}
impl<S> Eq for LazyReader<S> where S: BlobStore + WeakPinStore + StorageFlush + Send + 'static {}

impl<S> BlobStoreGet for LazyReader<S>
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

impl<S> BlobStoreList for LazyReader<S>
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
    use futures::executor::block_on;
    use futures::task::{waker, ArcWake};
    use std::sync::atomic::AtomicUsize;

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
        let mut lazy = Lazy::new(MemoryRepo::default());
        let (blob, handle) = blob_of(b"resident");
        BlobStorePut::put::<UnknownBlob, _>(&mut lazy, blob).unwrap();

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let bytes: Bytes =
            BlobStoreGet::get(&reader, handle).expect("resident blob serves locally");
        assert_eq!(&bytes[..], b"resident");
        assert_eq!(lazy.weak_pins().unwrap().count(), 0, "a hit records no want");
    }

    /// The core contract of the sync probe: a miss returns `NotYet` with
    /// the want durably on record — visible through the same handle AND
    /// through a second `Pile` opened fresh on the same file (the reader
    /// path flushed).
    #[test]
    fn miss_records_durable_want_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy.pile");
        let mut lazy: Lazy<Pile> = Lazy::new(fresh_pile(&path));
        let (_, handle) = blob_of(b"wanted but absent");

        let reader = BlobStore::reader(&mut lazy).unwrap();
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
        let mut lazy = Lazy::new(FailingPins::default());
        let (_, handle) = blob_of(b"unrecordable");

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let err = BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle)
            .expect_err("miss on a pin-refusing store must error");
        assert!(
            matches!(err, WantGetError::WantRecord(WantRecordError::Pin(PinRefused))),
            "want-record failure must surface as WantRecord, got {err:?}"
        );
    }

    /// Same posture on the async read: it errors instead of suspending
    /// when the want cannot be recorded — a wait without a recorded want
    /// is a wait nobody will ever satisfy.
    #[test]
    fn async_want_record_failure_is_an_error() {
        let mut lazy = Lazy::new(FailingPins::default());
        let (_, handle) = blob_of(b"unrecordable");

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let err = block_on(AsyncBlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle))
            .expect_err("miss on a pin-refusing store must error, not suspend");
        assert!(
            matches!(err, WantWaitError::WantRecord(WantRecordError::Pin(PinRefused))),
            "want-record failure must surface as WantRecord, got {err:?}"
        );
    }

    // ── Async waiting read ───────────────────────────────────────────

    /// Wake-counting waker: lets a test poll a future by hand and assert
    /// exactly when it was signaled.
    struct CountingWaker(AtomicUsize);
    impl ArcWake for CountingWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A present blob resolves the async read immediately — first poll,
    /// no want recorded, no suspension.
    #[test]
    fn async_get_present_resolves_immediately() {
        let mut lazy = Lazy::new(MemoryRepo::default());
        let (blob, handle) = blob_of(b"already here");
        BlobStorePut::put::<UnknownBlob, _>(&mut lazy, blob).unwrap();

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let bytes: Bytes =
            block_on(AsyncBlobStoreGet::get(&reader, handle)).expect("present blob resolves");
        assert_eq!(&bytes[..], b"already here");
        assert_eq!(lazy.weak_pins().unwrap().count(), 0, "a hit records no want");
    }

    /// The in-process wake path: the first poll records the durable want
    /// and parks; a `put` through the same `Lazy` wakes the waiter;
    /// the re-poll resolves.
    #[test]
    fn async_get_wakes_on_in_process_put() {
        let mut lazy = Lazy::new(MemoryRepo::default());
        let (blob, handle) = blob_of(b"lands in process");

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let mut fut =
            Box::pin(AsyncBlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle));

        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = waker(counter.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "absent blob suspends"
        );
        let wants: Vec<_> = lazy.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(wants, vec![handle], "first pending poll recorded the want");
        assert_eq!(counter.0.load(Ordering::SeqCst), 0, "no wake before the put");

        BlobStorePut::put::<UnknownBlob, _>(&mut lazy, blob).unwrap();
        assert!(
            counter.0.load(Ordering::SeqCst) >= 1,
            "in-process put wakes the parked waiter"
        );

        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(bytes)) => assert_eq!(&bytes[..], b"lands in process"),
            other => panic!("re-poll after landing must resolve, got {other:?}"),
        }
    }

    /// Cancellation safety: dropping a suspended read abandons the wait,
    /// but the durable want REMAINS recorded.
    #[test]
    fn dropped_wait_keeps_want_recorded() {
        let mut lazy = Lazy::new(MemoryRepo::default());
        let (_, handle) = blob_of(b"nobody lands this");

        let reader = BlobStore::reader(&mut lazy).unwrap();
        {
            let mut fut =
                Box::pin(AsyncBlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle));
            let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
            let waker = waker(counter);
            let mut cx = Context::from_waker(&waker);
            assert!(fut.as_mut().poll(&mut cx).is_pending());
            // fut dropped here — the wait is abandoned.
        }

        let wants: Vec<_> = lazy.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(wants, vec![handle], "the want outlives the dropped future");
    }

    /// The cross-process path: a blob landed by a *second pile handle*
    /// on the same file (standing in for a sync daemon in another
    /// process) is observed by the cadence re-check — the suspended read
    /// resolves without any in-process put.
    #[test]
    fn async_get_resolves_once_landed_by_second_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.pile");
        let mut lazy: Lazy<Pile> = Lazy::new(fresh_pile(&path));
        let (blob, handle) = blob_of(b"landed later");

        let writer_path = path.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            let mut pile = Pile::open(&writer_path).unwrap();
            pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.close().unwrap();
        });

        let reader = BlobStore::reader(&mut lazy).unwrap();
        let bytes: Bytes = block_on(AsyncBlobStoreGet::get(&reader, handle))
            .expect("blob appears and the wait resolves");
        assert_eq!(&bytes[..], b"landed later");
        writer.join().unwrap();

        drop(reader);
        lazy.into_store().close().unwrap();
    }

    /// A corrupt pile tail fails loud and immediately
    /// (`WantWaitError::Store(ReadError)`) — never auto-restored, never
    /// suspended on.
    #[test]
    fn async_get_corrupt_tail_fails_loud() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.pile");
        let mut lazy: Lazy<Pile> = Lazy::new(fresh_pile(&path));
        let (_, handle) = blob_of(b"unreachable");
        let reader = BlobStore::reader(&mut lazy).unwrap();

        // Corrupt the tail out-of-band: garbage that matches no record
        // magic.
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xAB; 256]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let err = block_on(AsyncBlobStoreGet::get::<Bytes, UnknownBlob>(&reader, handle))
            .expect_err("corrupt tail must fail");
        assert!(
            matches!(
                err,
                WantWaitError::Store(crate::repo::pile::ReadError::CorruptPile { .. })
            ),
            "expected Store(CorruptPile), got {err:?}"
        );
    }

    /// A generic async consumer can drive a `Lazy` end to end through
    /// the async traits alone: put → reader → get → blobs.
    #[test]
    fn async_traits_roundtrip() {
        let mut lazy = Lazy::new(MemoryRepo::default());
        let (blob, _) = blob_of(b"through the async surface");

        let handle =
            block_on(AsyncBlobStorePut::put::<UnknownBlob, _>(&mut lazy, blob)).unwrap();
        let reader = block_on(AsyncBlobStore::reader(&mut lazy)).unwrap();
        let bytes: Bytes = block_on(AsyncBlobStoreGet::get(&reader, handle)).unwrap();
        assert_eq!(&bytes[..], b"through the async surface");

        let listed: Vec<_> = block_on(AsyncBlobStoreList::blobs(&reader))
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert_eq!(listed, vec![handle]);
    }

    // Statically assert the futures are `Send` — required by the RPITIT
    // contract of the async traits. If the waiting future ever captured
    // something non-Send (the phantom-typed handle, a reader snapshot),
    // this would stop compiling.
    fn _assert_send<F: Send>(_: F) {}
    #[allow(dead_code)]
    fn _send_proof(
        lazy: &mut Lazy<MemoryRepo>,
        reader: &LazyReader<MemoryRepo>,
        handle: Inline<Handle<UnknownBlob>>,
    ) {
        _assert_send(AsyncBlobStoreGet::get::<Bytes, UnknownBlob>(reader, handle));
        _assert_send(AsyncBlobStoreList::blobs(reader));
        _assert_send(AsyncBlobStorePut::put::<UnknownBlob, _>(
            lazy,
            Blob::<UnknownBlob>::new(Bytes::from_source(&b"x"[..])),
        ));
        _assert_send(AsyncBlobStore::reader(lazy));
    }
}
