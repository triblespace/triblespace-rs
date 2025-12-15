use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::id::Id;
use crate::prelude::blobschemas::SimpleArchive;
use crate::repo::BlobStore;
use crate::repo::BlobStorePut;
use crate::repo::BranchStore;
use crate::repo::PushResult;
use crate::value::schemas::hash::Handle;
use crate::value::schemas::hash::HashProtocol;
use crate::value::Value;
use crate::value::ValueSchema;

/// Store that delegates blob and branch operations to two independent stores.
///
/// This allows mixing different storage implementations in one repository,
/// e.g. an on-disk blob store with an in-memory branch store.
#[derive(Debug)]
pub struct HybridStore<B, R> {
    /// Storage for commit, content and metadata blobs.
    pub blobs: B,
    /// Storage for branch heads.
    pub branches: R,
}

impl<B, R> HybridStore<B, R> {
    /// Creates a new `HybridStore` from the given blob and branch stores.
    pub fn new(blobs: B, branches: R) -> Self {
        Self { blobs, branches }
    }
}

impl<H, B, R> BlobStorePut<H> for HybridStore<B, R>
where
    H: HashProtocol,
    B: BlobStorePut<H>,
{
    type PutError = B::PutError;

    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<H, S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<H, S>: ValueSchema,
    {
        self.blobs.put(item)
    }
}

impl<H, B, R> BlobStore<H> for HybridStore<B, R>
where
    H: HashProtocol,
    B: BlobStore<H>,
{
    type Reader = B::Reader;
    type ReaderError = B::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.blobs.reader()
    }
}

impl<H, B, R> BranchStore<H> for HybridStore<B, R>
where
    H: HashProtocol,
    R: BranchStore<H>,
{
    type BranchesError = R::BranchesError;
    type HeadError = R::HeadError;
    type UpdateError = R::UpdateError;

    type ListIter<'a>
        = R::ListIter<'a>
    where
        R: 'a,
        B: 'a;

    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError> {
        self.branches.branches()
    }

    fn head(&mut self, id: Id) -> Result<Option<Value<Handle<H, SimpleArchive>>>, Self::HeadError> {
        self.branches.head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<H, SimpleArchive>>>,
        new: Value<Handle<H, SimpleArchive>>,
    ) -> Result<PushResult<H>, Self::UpdateError> {
        self.branches.update(id, old, new)
    }
}
