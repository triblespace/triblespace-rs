use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::id::Id;
use crate::prelude::blobencodings::SimpleArchive;
use crate::repo::BlobStore;
use crate::repo::BlobStorePut;
use crate::repo::BranchStore;
use crate::repo::PushResult;
use crate::value::encodings::hash::Handle;
use crate::value::Inline;
use crate::value::InlineEncoding;

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
    /// Creates a new [`HybridStore`] from the given blob and branch stores.
    pub fn new(blobs: B, branches: R) -> Self {
        Self { blobs, branches }
    }
}

impl<B, R> BlobStorePut for HybridStore<B, R>
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

impl<B, R> BlobStore for HybridStore<B, R>
where
    B: BlobStore,
{
    type Reader = B::Reader;
    type ReaderError = B::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.blobs.reader()
    }
}

impl<B, R> BranchStore for HybridStore<B, R>
where
    R: BranchStore,
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

    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        self.branches.head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        self.branches.update(id, old, new)
    }
}
