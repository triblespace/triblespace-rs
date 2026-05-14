use std::collections::HashMap;
use std::convert::Infallible;

use crate::blob::schemas::UnknownBlob;
use crate::blob::BlobSchema;
use crate::blob::MemoryBlobStore;
use crate::blob::IntoBlob;
use crate::prelude::blobschemas::SimpleArchive;
use crate::prelude::*;
use crate::repo::BranchStore;
use crate::repo::PushResult;

use crate::value::schemas::hash::Handle;
use crate::value::InlineSchema;

/// Simple in-memory implementation of [`BlobStore`] and [`BranchStore`].
///
/// Useful for unit tests or ephemeral repositories where persistence is not
/// required.
#[derive(Debug, Default)]
pub struct MemoryRepo {
    /// In-memory blob store for all repository blobs.
    pub blobs: MemoryBlobStore,
    /// Map from branch id to the handle of its current head commit.
    pub branches: HashMap<Id, Inline<Handle<SimpleArchive>>>,
}

impl crate::repo::BlobStorePut for MemoryRepo {
    type PutError = <MemoryBlobStore as crate::repo::BlobStorePut>::PutError;
    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineSchema,
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

impl BranchStore for MemoryRepo {
    type BranchesError = Infallible;
    type HeadError = Infallible;
    type UpdateError = Infallible;

    type ListIter<'a> = std::vec::IntoIter<Result<Id, Self::BranchesError>>;

    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError> {
        Ok(self
            .branches
            .keys()
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>()
            .into_iter())
    }

    fn head(
        &mut self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        Ok(self.branches.get(&id).cloned())
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let current = self.branches.get(&id);
        if current != old.as_ref() {
            return Ok(PushResult::Conflict(current.cloned()));
        }
        match new {
            Some(new) => {
                self.branches.insert(id, new);
            }
            None => {
                self.branches.remove(&id);
            }
        }
        Ok(PushResult::Success())
    }
}

impl crate::repo::StorageClose for MemoryRepo {
    type Error = Infallible;

    fn close(self) -> Result<(), Self::Error> {
        // Nothing to do for the in-memory backend.
        Ok(())
    }
}
