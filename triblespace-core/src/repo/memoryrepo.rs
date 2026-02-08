use std::collections::HashMap;
use std::convert::Infallible;

use crate::blob::schemas::UnknownBlob;
use crate::blob::BlobSchema;
use crate::blob::MemoryBlobStore;
use crate::blob::ToBlob;
use crate::prelude::blobschemas::SimpleArchive;
use crate::prelude::*;
use crate::repo::BranchStore;
use crate::repo::PushResult;
use crate::value::schemas::hash::Blake3;

use crate::value::schemas::hash::Handle;
use crate::value::ValueSchema;

#[derive(Debug)]
/// Simple in-memory implementation of [`BlobStore`] and [`BranchStore`].
///
/// Useful for unit tests or ephemeral repositories where persistence is not
/// required.
#[derive(Default)]
pub struct MemoryRepo {
    pub blobs: MemoryBlobStore<Blake3>,
    pub branches: HashMap<Id, Value<Handle<Blake3, SimpleArchive>>>,
}

impl crate::repo::BlobStorePut<Blake3> for MemoryRepo {
    type PutError = <MemoryBlobStore<Blake3> as crate::repo::BlobStorePut<Blake3>>::PutError;
    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<Blake3, S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<Blake3, S>: ValueSchema,
    {
        self.blobs.put(item)
    }
}

impl crate::repo::BlobStore<Blake3> for MemoryRepo {
    type Reader = <MemoryBlobStore<Blake3> as crate::repo::BlobStore<Blake3>>::Reader;
    type ReaderError = <MemoryBlobStore<Blake3> as crate::repo::BlobStore<Blake3>>::ReaderError;
    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.blobs.reader()
    }
}

impl crate::repo::BlobStoreKeep<Blake3> for MemoryRepo {
    fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Value<Handle<Blake3, UnknownBlob>>>,
    {
        self.blobs.keep(handles);
    }
}

impl BranchStore<Blake3> for MemoryRepo {
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
    ) -> Result<Option<Value<Handle<Blake3, SimpleArchive>>>, Self::HeadError> {
        Ok(self.branches.get(&id).cloned())
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<Blake3, SimpleArchive>>>,
        new: Option<Value<Handle<Blake3, SimpleArchive>>>,
    ) -> Result<PushResult<Blake3>, Self::UpdateError> {
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
