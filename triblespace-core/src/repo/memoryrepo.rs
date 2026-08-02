use std::collections::HashMap;
use std::convert::Infallible;

use crate::blob::encodings::UnknownBlob;
use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::blob::MemoryBlobStore;
use crate::prelude::blobencodings::SimpleArchive;
use crate::prelude::*;
use crate::repo::pin_assertion::{
    PinAssertion, PinAssertionKeyCollision, PinAssertionSnapshot, PinAssertionStore,
};
use crate::repo::want::{WantCachePolicy, WantCachePolicySource};
use crate::repo::PinStore;
use crate::repo::PushResult;

use crate::inline::encodings::hash::Handle;
use crate::inline::InlineEncoding;

/// Simple in-memory blob, assertion, and local-pin store.
///
/// Useful for unit tests or ephemeral repositories where persistence is not
/// required.
#[derive(Debug, Default)]
pub struct MemoryRepo {
    /// In-memory blob store for all repository blobs.
    pub blobs: MemoryBlobStore,
    /// Map from local pin id to its current arbitrary SimpleArchive value.
    pub pins: HashMap<Id, Inline<Handle<SimpleArchive>>>,
    /// Generic grow-only asserted pins.
    pin_assertions: PinAssertionSnapshot,
}

impl PinAssertionStore for MemoryRepo {
    type Error = PinAssertionKeyCollision;

    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
        Ok(self.pin_assertions.clone())
    }

    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
        self.pin_assertions.insert(assertion)
    }
}

impl WantCachePolicySource for MemoryRepo {
    fn want_cache_policy(&self) -> WantCachePolicy {
        WantCachePolicy::unbounded()
    }
}

impl crate::repo::BlobStorePut for MemoryRepo {
    type PutError = <MemoryBlobStore as crate::repo::BlobStorePut>::PutError;
    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.blobs.put(item)
    }
}

impl crate::repo::BlobStore for MemoryRepo {
    type Reader = <MemoryBlobStore as crate::repo::BlobStore>::Reader;
    type ReaderError = <MemoryBlobStore as crate::repo::BlobStore>::ReaderError;
    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.blobs.reader()
    }
}

impl crate::repo::BlobStoreKeep for MemoryRepo {
    fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Inline<Handle<UnknownBlob>>>,
    {
        self.blobs.keep(handles);
    }
}

impl PinStore for MemoryRepo {
    type PinsError = Infallible;
    type HeadError = Infallible;
    type UpdateError = Infallible;

    type ListIter<'a> = std::vec::IntoIter<Result<Id, Self::PinsError>>;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        // Sorted (not HashMap order): pin iteration feeds serving-snapshot and
        // policy construction; HashMap's per-instance seed would reorder each
        // run and break deterministic simulation replay. Pile's PATCH-backed
        // pins() is already byte-ordered for the same reason.
        let mut ids: Vec<Id> = self.pins.keys().cloned().collect();
        ids.sort();
        Ok(ids.into_iter().map(Ok).collect::<Vec<_>>().into_iter())
    }

    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        Ok(self.pins.get(&id).cloned())
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let current = self.pins.get(&id);
        if current != old.as_ref() {
            return Ok(PushResult::Conflict(current.cloned()));
        }
        match new {
            Some(new) => {
                self.pins.insert(id, new);
            }
            None => {
                self.pins.remove(&id);
            }
        }
        Ok(PushResult::Success())
    }
}

impl crate::repo::StorageFlush for MemoryRepo {
    type Error = Infallible;

    fn flush(&mut self) -> Result<(), Self::Error> {
        // In-memory state has no sync point; durability is exactly the
        // process lifetime, same as the blobs themselves.
        Ok(())
    }
}

impl crate::repo::StorageClose for MemoryRepo {
    type Error = Infallible;

    fn close(self) -> Result<(), Self::Error> {
        // Nothing to do for the in-memory backend.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::pin_assertion::{PinHandle, SubsumptionLabel, ValueHandle};
    use ed25519_dalek::SigningKey;

    #[test]
    fn pin_assertion_store_is_grow_only_idempotent_and_snapshot_coherent() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let first = PinAssertion::sign(
            &key,
            PinHandle::from_raw([11; 32]),
            ValueHandle::from_raw([19; 32]),
            SubsumptionLabel::from_raw([1; 32]),
        );
        let mut repo = MemoryRepo::default();

        repo.append_pin_assertion(first).unwrap();
        repo.append_pin_assertion(first).unwrap();

        let snapshot = repo.pin_assertion_snapshot().unwrap();
        assert_eq!(snapshot.iter().copied().collect::<Vec<_>>(), vec![first]);

        let second = PinAssertion::sign(
            &key,
            PinHandle::from_raw([11; 32]),
            ValueHandle::from_raw([23; 32]),
            SubsumptionLabel::from_raw([2; 32]),
        );
        repo.append_pin_assertion(second).unwrap();
        assert_eq!(snapshot.len(), 1, "an earlier snapshot stays coherent");
        assert_eq!(repo.pin_assertion_snapshot().unwrap().len(), 2);
    }
}
