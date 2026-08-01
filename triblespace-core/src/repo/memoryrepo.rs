use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;

use crate::blob::encodings::UnknownBlob;
use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::blob::MemoryBlobStore;
use crate::prelude::blobencodings::SimpleArchive;
use crate::prelude::*;
use crate::repo::branch_assertion::{
    AssertionKeyCollision, BranchAssertion, BranchAssertionSnapshot, BranchAssertionStore,
};
use crate::repo::PinStore;
use crate::repo::PushResult;
use crate::repo::WeakPinStore;

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
    /// Verified grow-only branch assertions.
    assertions: BranchAssertionSnapshot,
    /// LWW-resolved weak-pin set (see [`WeakPinStore`]). In memory the
    /// last-writer-wins resolution is just insert/remove. Weak pins here
    /// are exactly as ephemeral as the blobs themselves — the trait is a
    /// capability, durability is the store's own property.
    pub weak: HashSet<Inline<Handle<UnknownBlob>>>,
}

impl BranchAssertionStore for MemoryRepo {
    type Error = AssertionKeyCollision;

    fn assertion_snapshot(&mut self) -> Result<BranchAssertionSnapshot, Self::Error> {
        Ok(self.assertions.clone())
    }

    fn append_assertion(&mut self, assertion: BranchAssertion) -> Result<(), Self::Error> {
        self.assertions.insert(assertion)
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
        // Sorted (not HashMap order): pin iteration order feeds
        // gossip-publish order and snapshot construction; HashMap's
        // per-instance seed would make every run reorder them, which
        // breaks deterministic simulation replay. Pile's PATCH-backed
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

impl WeakPinStore for MemoryRepo {
    type WeakPinError = Infallible;

    type WeakListIter<'a> =
        std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, Self::WeakPinError>>;

    fn pin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.weak.insert(handle.transmute());
        Ok(())
    }

    fn unpin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.weak.remove(&handle.transmute());
        Ok(())
    }

    fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError> {
        // Sorted for the same reason as `pins()`: weak-pin enumeration
        // feeds sync-daemon fetch order, and HashSet's per-instance seed
        // would break deterministic simulation replay.
        let mut handles: Vec<Inline<Handle<UnknownBlob>>> = self.weak.iter().copied().collect();
        handles.sort();
        Ok(handles.into_iter().map(Ok).collect::<Vec<_>>().into_iter())
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
    use crate::blob::encodings::longstring::LongString;
    use ed25519_dalek::SigningKey;

    fn handle(byte: u8) -> Inline<Handle<UnknownBlob>> {
        Inline::new([byte; 32])
    }

    #[test]
    fn assertion_store_is_grow_only_idempotent_and_snapshot_coherent() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let assertion = BranchAssertion::sign(
            &key,
            Inline::<Handle<LongString>>::new([11; 32]),
            Inline::<Handle<SimpleArchive>>::new([19; 32]),
        );
        let mut repo = MemoryRepo::default();

        repo.append_assertion(assertion).unwrap();
        repo.append_assertion(assertion).unwrap();

        let first = repo.assertion_snapshot().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first.iter().copied().collect::<Vec<_>>(), vec![assertion]);

        let second = BranchAssertion::sign(
            &key,
            Inline::<Handle<LongString>>::new([11; 32]),
            Inline::<Handle<SimpleArchive>>::new([23; 32]),
        );
        repo.append_assertion(second).unwrap();
        assert_eq!(first.len(), 1, "an earlier snapshot stays coherent");
        assert_eq!(repo.assertion_snapshot().unwrap().len(), 2);
    }

    /// Weak pins resolve last-writer-wins: pin → listed, unpin →
    /// gone, re-pin → listed again. Enumeration is sorted (stable
    /// across runs despite HashSet backing).
    #[test]
    fn weak_pins_lww_roundtrip() {
        let mut repo = MemoryRepo::default();
        assert_eq!(repo.weak_pins().unwrap().count(), 0);

        repo.pin_weak(handle(2)).unwrap();
        repo.pin_weak(handle(1)).unwrap();
        // Re-pinning an already-pinned handle is idempotent.
        repo.pin_weak(handle(1)).unwrap();
        let pins: Vec<_> = repo.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(pins, vec![handle(1), handle(2)], "sorted enumeration");

        repo.unpin_weak(handle(1)).unwrap();
        let pins: Vec<_> = repo.weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(pins, vec![handle(2)]);

        // A later weak pin wins over the earlier unpin.
        repo.pin_weak(handle(1)).unwrap();
        assert_eq!(repo.weak_pins().unwrap().count(), 2);
    }
}
