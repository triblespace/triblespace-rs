use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::repo::pin_assertion::{PinAssertion, PinAssertionSnapshot, PinAssertionStore};
use crate::repo::want::{WantCachePolicy, WantCachePolicySource};
use crate::repo::BlobStore;
use crate::repo::BlobStorePut;
use crate::repo::StorageClose;
use crate::repo::StorageFlush;
use std::error::Error;
use std::fmt;

/// Failure while closing one or both halves of a [`HybridStore`].
///
/// Closing always attempts both stores. If both fail, [`Self::Both`] retains
/// both original errors so callers can diagnose every cleanup failure.
#[derive(Debug)]
pub enum HybridCloseError<BlobError, AssertionError> {
    /// Closing the blob store failed.
    Blobs(BlobError),
    /// Closing the assertion store failed.
    Assertions(AssertionError),
    /// Closing both stores failed.
    Both {
        /// Error returned by the blob store.
        blobs: BlobError,
        /// Error returned by the assertion store.
        assertions: AssertionError,
    },
}

impl<BlobError, AssertionError> fmt::Display for HybridCloseError<BlobError, AssertionError>
where
    BlobError: fmt::Display,
    AssertionError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blobs(error) => write!(f, "failed to close blob store: {error}"),
            Self::Assertions(error) => write!(f, "failed to close assertion store: {error}"),
            Self::Both { blobs, assertions } => write!(
                f,
                "failed to close blob store ({blobs}) and assertion store ({assertions})"
            ),
        }
    }
}

impl<BlobError, AssertionError> Error for HybridCloseError<BlobError, AssertionError>
where
    BlobError: Error,
    AssertionError: Error,
{
}

/// Store that delegates blobs and signed assertions to independent stores.
///
/// This allows mixing different storage implementations in one repository,
/// e.g. an on-disk blob store with an in-memory assertion store.
#[derive(Debug)]
pub struct HybridStore<B, A> {
    /// Storage for commit, content and metadata blobs.
    pub blobs: B,
    /// Storage for generic grow-only pin assertions.
    pub assertions: A,
}

impl<B, A> HybridStore<B, A> {
    /// Creates a new [`HybridStore`] from the given blob and assertion stores.
    pub fn new(blobs: B, assertions: A) -> Self {
        Self { blobs, assertions }
    }
}

impl<B, A> BlobStorePut for HybridStore<B, A>
where
    B: BlobStorePut,
{
    type PutError = B::PutError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.blobs.put(item)
    }
}

impl<B, A> BlobStore for HybridStore<B, A>
where
    B: BlobStore,
{
    type Reader = B::Reader;
    type ReaderError = B::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.blobs.reader()
    }
}

impl<B, A> PinAssertionStore for HybridStore<B, A>
where
    A: PinAssertionStore,
{
    type Error = A::Error;

    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
        self.assertions.pin_assertion_snapshot()
    }

    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
        self.assertions.append_pin_assertion(assertion)
    }
}

impl<B, A> WantCachePolicySource for HybridStore<B, A>
where
    B: WantCachePolicySource,
{
    fn want_cache_policy(&self) -> WantCachePolicy {
        self.blobs.want_cache_policy()
    }
}

impl<B, A> StorageFlush for HybridStore<B, A>
where
    B: StorageFlush,
{
    type Error = B::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        // Both assertion-store append operations are already durability
        // boundaries. Only pending blob writes need an explicit flush here.
        self.blobs.flush()
    }
}

impl<B, A> StorageClose for HybridStore<B, A>
where
    B: StorageClose,
    A: StorageClose,
{
    type Error = HybridCloseError<B::Error, A::Error>;

    fn close(self) -> Result<(), Self::Error> {
        let Self { blobs, assertions } = self;
        let blobs = blobs.close();
        let assertions = assertions.close();

        match (blobs, assertions) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(HybridCloseError::Blobs(error)),
            (Ok(()), Err(error)) => Err(HybridCloseError::Assertions(error)),
            (Err(blobs), Err(assertions)) => Err(HybridCloseError::Both { blobs, assertions }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::fmt;
    use std::rc::Rc;

    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::pin_assertion::{PinHandle, SubsumptionLabel, ValueHandle};

    #[derive(Debug)]
    struct FlushProbe(Rc<Cell<usize>>);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct CloseFailure(&'static str);

    impl fmt::Display for CloseFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for CloseFailure {}

    #[derive(Debug)]
    struct CloseProbe {
        calls: Rc<Cell<usize>>,
        error: Option<CloseFailure>,
    }

    impl StorageClose for CloseProbe {
        type Error = CloseFailure;

        fn close(self) -> Result<(), Self::Error> {
            self.calls.set(self.calls.get() + 1);
            self.error.map_or(Ok(()), Err)
        }
    }

    impl StorageFlush for FlushProbe {
        type Error = Infallible;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.0.set(self.0.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn delegates_generic_pin_assertions_to_the_assertion_store() {
        let assertion = PinAssertion::sign(
            &SigningKey::from_bytes(&[7; 32]),
            PinHandle::from_raw([11; 32]),
            ValueHandle::from_raw([19; 32]),
            SubsumptionLabel::from_raw([3; 32]),
        );
        let mut hybrid = HybridStore::new((), MemoryRepo::default());

        hybrid.append_pin_assertion(assertion).unwrap();
        hybrid.append_pin_assertion(assertion).unwrap();

        let snapshot = hybrid.pin_assertion_snapshot().unwrap();
        assert_eq!(
            snapshot.iter().copied().collect::<Vec<_>>(),
            vec![assertion]
        );
    }

    #[test]
    fn flushes_only_the_blob_store() {
        let flushes = Rc::new(Cell::new(0));
        let mut hybrid = HybridStore::new(FlushProbe(flushes.clone()), ());

        hybrid.flush().unwrap();

        assert_eq!(flushes.get(), 1);
    }

    #[test]
    fn close_attempts_both_stores_and_preserves_both_failures() {
        let blob_calls = Rc::new(Cell::new(0));
        let assertion_calls = Rc::new(Cell::new(0));
        let hybrid = HybridStore::new(
            CloseProbe {
                calls: blob_calls.clone(),
                error: Some(CloseFailure("blob close")),
            },
            CloseProbe {
                calls: assertion_calls.clone(),
                error: Some(CloseFailure("assertion close")),
            },
        );

        let error = hybrid.close().unwrap_err();

        assert_eq!(blob_calls.get(), 1);
        assert_eq!(assertion_calls.get(), 1);
        assert!(matches!(
            error,
            HybridCloseError::Both {
                blobs: CloseFailure("blob close"),
                assertions: CloseFailure("assertion close"),
            }
        ));
    }
}
