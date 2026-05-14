use std::sync::Arc;

use quick_cache::sync::Cache;

use crate::blob::BlobSchema;
use crate::blob::TryFromBlob;
use crate::repo::BlobStoreGet;
use crate::value::schemas::hash::Handle;
use crate::value::Value;
use crate::value::ValueSchema;

const DEFAULT_BLOB_CACHE_CAPACITY: usize = 256;

/// Lazy cache for blob conversions keyed by blob handle.
pub struct BlobCache<B, S, T>
where
    B: BlobStoreGet,
    S: BlobSchema + 'static,
    T: TryFromBlob<S>,
    Handle<S>: ValueSchema,
{
    blobs: B,
    by_handle: Cache<Value<Handle<S>>, Arc<T>>,
}

impl<B, S, T> BlobCache<B, S, T>
where
    B: BlobStoreGet,
    S: BlobSchema + 'static,
    T: TryFromBlob<S>,
    Handle<S>: ValueSchema,
{
    /// Creates a new cache backed by `blobs` with the default capacity.
    pub fn new(blobs: B) -> Self {
        Self::with_capacity(blobs, DEFAULT_BLOB_CACHE_CAPACITY)
    }

    /// Creates a new cache backed by `blobs` with the given entry capacity.
    pub fn with_capacity(blobs: B, capacity: usize) -> Self {
        Self {
            blobs,
            by_handle: Cache::new(capacity),
        }
    }

    /// Returns the cached value for `handle`, fetching and converting it on a cache miss.
    pub fn get(&self, handle: Value<Handle<S>>) -> Result<Arc<T>, B::GetError<T::Error>> {
        let blobs = &self.blobs;
        self.by_handle.get_or_insert_with(&handle, || {
            let value = blobs.get::<T, S>(handle)?;
            Ok(Arc::new(value))
        })
    }
}
