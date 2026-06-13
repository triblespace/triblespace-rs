//! Cache tiers for the lazy-replication store stack.
//!
//! The `Cache` tier in `Peer<Durable, Cache>` is where read-miss swarm
//! fetches land. It is `BlobStore + BlobStorePut` but NOT `PinStore`
//! (caches do not pin — pins live in Durable). Eviction is always
//! safe: anything worth keeping was pinned into Durable, and
//! content-addressing makes a cache miss re-fetchable. "Pins are
//! promises, caches are free."
//!
//! Two implementations:
//! - [`BoundedBlobStore`]: a capacity-bounded in-memory tier. FIFO
//!   eviction for now (LRU is a refinement — see note on `touch`).
//! - [`NullCache`]: drops every insert. The relay's tier
//!   (`Peer<Pile, NullCache>`) — it pins everything, so reads always
//!   hit Durable and a cache would be dead weight.

use std::collections::{HashSet, VecDeque};

use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{BlobEncoding, IntoBlob, MemoryBlobStore};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, InlineEncoding};
use triblespace_core::repo::{BlobStore, BlobStorePut};

use crate::protocol::RawHash;

/// A capacity-bounded in-memory blob tier.
///
/// Holds at most `capacity` blobs; inserting past that evicts the
/// oldest (FIFO). Reads go through the inner [`MemoryBlobStore`]'s
/// snapshot reader (cheap PATCH clone). Eviction is harmless by the
/// cache contract — durability lives in Durable.
pub struct BoundedBlobStore {
    inner: MemoryBlobStore,
    /// Insertion order, oldest at the front — the FIFO eviction queue.
    order: VecDeque<RawHash>,
    /// Membership, so a re-insert of a present blob does not push a
    /// duplicate order entry (content-addressed: same handle ⇒ same
    /// bytes, so re-insert is a no-op for contents).
    present: HashSet<RawHash>,
    capacity: usize,
}

impl std::fmt::Debug for BoundedBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedBlobStore")
            .field("len", &self.present.len())
            .field("capacity", &self.capacity)
            .finish()
    }
}

impl BoundedBlobStore {
    /// A cache holding at most `capacity` blobs. `capacity == 0` makes
    /// every insert immediately evict (degenerate; prefer [`NullCache`]).
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: MemoryBlobStore::new(),
            order: VecDeque::new(),
            present: HashSet::new(),
            capacity,
        }
    }

    /// Number of blobs currently held.
    pub fn len(&self) -> usize {
        self.present.len()
    }

    /// True iff empty.
    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    /// True iff `hash` is currently cached.
    pub fn contains(&self, hash: &RawHash) -> bool {
        self.present.contains(hash)
    }

    /// Record a fresh insertion of `hash` and evict down to capacity.
    /// (LRU refinement note: on a *hit*, a real LRU would move the
    /// hash to the back of `order` here; FIFO does not, so hot-but-old
    /// blobs can be evicted. Correctness is unaffected — eviction is
    /// always safe — so FIFO is the honest first cut.)
    fn track_and_evict(&mut self, hash: RawHash) {
        if !self.present.insert(hash) {
            return; // already present — content-addressed, no-op
        }
        self.order.push_back(hash);
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.present.remove(&old);
            } else {
                break;
            }
        }
        if self.order.len() < self.present.len() + 1 {
            // Survivors changed — retain only what is still present.
            // `MemoryBlobStore::keep` rebuilds the index with just
            // these handles (O(survivors), bounded by capacity).
            let survivors: Vec<Inline<Handle<UnknownBlob>>> = self
                .present
                .iter()
                .map(|h| Inline::new(*h))
                .collect();
            self.inner.keep(survivors);
        }
    }
}

impl BlobStorePut for BoundedBlobStore {
    type PutError = std::convert::Infallible;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let handle = self.inner.put(item)?;
        self.track_and_evict(handle.raw);
        Ok(handle)
    }
}

impl BlobStore for BoundedBlobStore {
    type Reader = <MemoryBlobStore as BlobStore>::Reader;
    type ReaderError = <MemoryBlobStore as BlobStore>::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.inner.reader()
    }
}

/// A cache tier that discards every insert. Reads always miss; the
/// reader is permanently empty. For the relay (`Peer<Pile, NullCache>`)
/// which pins everything into Durable and never needs a transient
/// tier — the empty type makes "no cache" explicit in the deployment.
#[derive(Debug, Default)]
pub struct NullCache {
    empty: MemoryBlobStore,
}

impl NullCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStorePut for NullCache {
    type PutError = std::convert::Infallible;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        // Compute the handle (content-addressed) but store nothing.
        Ok(item.to_blob().get_handle())
    }
}

impl BlobStore for NullCache {
    type Reader = <MemoryBlobStore as BlobStore>::Reader;
    type ReaderError = <MemoryBlobStore as BlobStore>::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.empty.reader()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::trible::TribleSet;

    fn blob(tag: u8) -> triblespace_core::blob::Blob<SimpleArchive> {
        use triblespace_core::id::ExclusiveId;
        use triblespace_core::id::Id;
        use triblespace_core::macros::entity;
        let e = Id::new([tag; 16]).unwrap();
        let ts: TribleSet = entity! {
            ExclusiveId::force_ref(&e) @
            triblespace_core::metadata::tag: Id::new([tag.wrapping_add(7).max(1); 16]).unwrap(),
        }
        .into();
        ts.to_blob()
    }

    #[test]
    fn put_then_get_roundtrips() {
        let mut c = BoundedBlobStore::new(4);
        let b = blob(1);
        let h = c.put::<SimpleArchive, _>(b.clone()).unwrap();
        let r = c.reader().unwrap();
        let got: triblespace_core::blob::Blob<SimpleArchive> = r.get(h).unwrap();
        assert_eq!(got.bytes, b.bytes);
        assert!(c.contains(&h.raw));
    }

    #[test]
    fn evicts_oldest_past_capacity() {
        let mut c = BoundedBlobStore::new(2);
        let h1 = c.put::<SimpleArchive, _>(blob(1)).unwrap();
        let h2 = c.put::<SimpleArchive, _>(blob(2)).unwrap();
        let h3 = c.put::<SimpleArchive, _>(blob(3)).unwrap();
        assert_eq!(c.len(), 2, "capacity bound holds");
        assert!(!c.contains(&h1.raw), "oldest evicted");
        assert!(c.contains(&h2.raw) && c.contains(&h3.raw));
        // The evicted blob is genuinely gone from the reader.
        let r = c.reader().unwrap();
        assert!(r
            .get::<triblespace_core::blob::Blob<SimpleArchive>, SimpleArchive>(h1)
            .is_err());
        assert!(r
            .get::<triblespace_core::blob::Blob<SimpleArchive>, SimpleArchive>(h3)
            .is_ok());
    }

    #[test]
    fn reinsert_is_idempotent_no_double_order() {
        let mut c = BoundedBlobStore::new(2);
        let h1 = c.put::<SimpleArchive, _>(blob(1)).unwrap();
        let _h2 = c.put::<SimpleArchive, _>(blob(2)).unwrap();
        // Re-insert h1: must NOT push a duplicate order entry (which
        // would then evict h2 spuriously).
        let _ = c.put::<SimpleArchive, _>(blob(1)).unwrap();
        assert_eq!(c.len(), 2);
        assert!(c.contains(&h1.raw));
    }

    #[test]
    fn null_cache_stores_nothing_but_returns_handle() {
        let mut c = NullCache::new();
        let b = blob(9);
        let h = c.put::<SimpleArchive, _>(b.clone()).unwrap();
        assert_eq!(h.raw, b.get_handle().raw, "handle still computed");
        let r = c.reader().unwrap();
        assert!(
            r.get::<triblespace_core::blob::Blob<SimpleArchive>, SimpleArchive>(h)
                .is_err(),
            "but nothing is stored"
        );
    }
}
