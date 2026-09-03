//! Service durable exact-content and collection-operation WANTs.
//!
//! Collection repair is a separate raw-record exchange. This reconciler
//! services durable exact-content and collection-operation demand without
//! deciding WRITE admission or claiming global absence:
//! an operation WANT is satisfied iff at least one matching local receipt is
//! visible; otherwise it remains pending while the local store evolves.
//! `Blob(H)` uses H-derived global provider discovery. No collection,
//! provenance guess, or ambient authorization participates in that exact read.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anybytes::Bytes;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::collection::{
    CollectionRead, CollectionRecord, CollectionRecordSelector, CollectionStore,
};
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, CapabilityProofStore, SnapshotSource, StorageFlush,
    StoreRead, WantRead, WantRequest, WantStore,
};

use crate::peer::Peer;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileStats {
    pub wants: usize,
    pub missing: usize,
    pub attempted: usize,
    pub fulfilled: usize,
    pub pending: usize,
}

struct WantState {
    last_attempt: crate::clock::Mono,
    backoff: Duration,
}

/// Retry state only. Durable demand and all answers remain in the store.
pub struct Reconciler {
    states: HashMap<WantRequest, WantState>,
    durable_blob_answers: HashSet<[u8; 32]>,
    initial_backoff: Duration,
    max_backoff: Duration,
    fetch_budget: Duration,
}

pub const RECONCILE_FETCH_DEADLINE: Duration = Duration::from_secs(30);

impl Default for Reconciler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reconciler {
    pub fn new() -> Self {
        Self::with_backoff(crate::RETRY_BACKOFF_BASE, crate::RETRY_BACKOFF_CAP)
    }

    pub fn with_backoff(initial: Duration, max: Duration) -> Self {
        Self {
            states: HashMap::new(),
            durable_blob_answers: HashSet::new(),
            initial_backoff: initial,
            max_backoff: max,
            fetch_budget: RECONCILE_FETCH_DEADLINE,
        }
    }

    pub fn with_fetch_budget(mut self, budget: Duration) -> Self {
        self.fetch_budget = budget;
        self
    }

    pub async fn tick<S>(&mut self, peer: &mut Peer<S>) -> ReconcileStats
    where
        S: BlobStore
            + CollectionStore
            + CapabilityProofStore
            + WantStore
            + StorageFlush
            + Send
            + 'static,
        S::Snapshot: StoreRead + BlobChildren,
    {
        let mut stats = ReconcileStats::default();

        // This is also the explicit external-Pile reobservation and inventory
        // admission boundary.
        peer.refresh();
        let snapshot = match peer.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "store snapshot unavailable; skipping reconcile pass"
                );
                return stats;
            }
        };
        let requests: Vec<WantRequest> = match snapshot
            .wants()
            .and_then(|wants| wants.collect::<Result<Vec<_>, _>>())
        {
            Ok(wants) => wants,
            Err(error) => {
                tracing::warn!(?error, "WANT observation failed; skipping reconcile pass");
                return stats;
            }
        };
        stats.wants = requests.len();

        let blob_wants: BTreeSet<_> = requests
            .iter()
            .copied()
            .filter(|request| request.blob_handle().is_some())
            .collect();
        let operation_wants: BTreeSet<_> = requests
            .iter()
            .copied()
            .filter(|request| {
                matches!(
                    request,
                    WantRequest::Merge { .. } | WantRequest::Derive { .. }
                )
            })
            .collect();

        // One native indexed union retains every conflicting answer. Empty is
        // only "not obtained yet", never proof that no answer exists.
        let selectors: BTreeSet<_> = operation_wants
            .iter()
            .copied()
            .map(CollectionRecordSelector::Operation)
            .collect();
        let answered_operations = match answered_operations(&snapshot, &selectors) {
            Ok(answered) => answered,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "operation receipt observation failed; skipping reconcile pass"
                );
                return ReconcileStats::default();
            }
        };
        let missing_operations = operation_wants
            .iter()
            .filter(|request| !answered_operations.contains(request))
            .count();

        let wanted_blob_handles: HashSet<_> = blob_wants
            .iter()
            .filter_map(|request| request.blob_handle().map(|handle| handle.raw))
            .collect();
        let visible_blobs: HashSet<_> = wanted_blob_handles
            .iter()
            .copied()
            .filter(|handle| {
                snapshot
                    .get::<Bytes, UnknownBlob>(triblespace_core::inline::Inline::new(*handle))
                    .is_ok()
            })
            .collect();

        self.durable_blob_answers.retain(|handle| {
            wanted_blob_handles.contains(handle) && visible_blobs.contains(handle)
        });
        let newly_visible: HashSet<_> = visible_blobs
            .difference(&self.durable_blob_answers)
            .copied()
            .collect();
        if !newly_visible.is_empty() {
            let durable = peer.store().flush();
            match durable {
                Ok(()) => {
                    self.durable_blob_answers
                        .extend(newly_visible.iter().copied());
                }
                Err(error) => tracing::warn!(
                    ?error,
                    "visible wanted blobs are not durable; keeping them pending"
                ),
            }
        }
        let missing_blobs: Vec<_> = blob_wants
            .iter()
            .copied()
            .filter(|request| {
                !self
                    .durable_blob_answers
                    .contains(&request.blob_handle().expect("blob WANT").raw)
            })
            .collect();
        stats.missing = missing_operations + missing_blobs.len();
        stats.pending = missing_operations;

        let outstanding: HashSet<_> = missing_blobs.iter().copied().collect();
        self.states
            .retain(|request, _| outstanding.contains(request));

        let started = crate::clock::mono_now();
        for request in missing_blobs {
            if self.states.get(&request).is_some_and(|state| {
                crate::clock::mono_now().duration_since(state.last_attempt) < state.backoff
            }) {
                stats.pending += 1;
                continue;
            }
            let remaining = self
                .fetch_budget
                .saturating_sub(crate::clock::mono_now().duration_since(started));
            if remaining.is_zero() {
                stats.pending += 1;
                continue;
            }
            let handle = request.blob_handle().expect("blob WANT").raw;
            stats.attempted += 1;
            let Some(bytes) = peer.fetch_blob_with_deadline(handle, remaining).await else {
                self.record_unavailable(request);
                stats.pending += 1;
                continue;
            };
            let landing = {
                let mut store = peer.store();
                match store.put::<UnknownBlob, Bytes>(bytes) {
                    Ok(actual) if actual.raw == handle => store
                        .flush()
                        .map_err(|error| format!("flush failed: {error:?}")),
                    Ok(_) => Err("blob store returned a different handle".to_owned()),
                    Err(error) => Err(format!("put failed: {error:?}")),
                }
            };
            if let Err(error) = landing {
                tracing::warn!(%error, "wanted blob landing failed; WANT remains pending");
                self.record_unavailable(request);
                stats.pending += 1;
                continue;
            }
            self.durable_blob_answers.insert(handle);
            self.states.remove(&request);
            stats.fulfilled += 1;
            peer.refresh();
        }
        stats
    }

    fn record_unavailable(&mut self, request: WantRequest) {
        let now = crate::clock::mono_now();
        match self.states.entry(request) {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.last_attempt = now;
                state.backoff = (state.backoff * 2).min(self.max_backoff);
            }
            Entry::Vacant(entry) => {
                entry.insert(WantState {
                    last_attempt: now,
                    backoff: self.initial_backoff,
                });
            }
        }
    }
}

fn answered_operations<R>(
    snapshot: &R,
    selectors: &BTreeSet<CollectionRecordSelector>,
) -> Result<HashSet<WantRequest>, R::RecordsError>
where
    R: CollectionRead,
{
    Ok(snapshot
        .select_records(selectors)?
        .into_iter()
        .filter_map(want_request_for_record)
        .collect())
}

fn want_request_for_record(record: CollectionRecord) -> Option<WantRequest> {
    match record {
        CollectionRecord::Commit(_) => None,
        CollectionRecord::Merge(merge) => {
            let (low, high) = merge.inputs();
            Some(WantRequest::merge(merge.collection(), low, high))
        }
        CollectionRecord::Derive(derive) => {
            Some(WantRequest::derive(derive.collection(), derive.input()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::collection::{CollectionDerive, CollectionMerge};
    use triblespace_core::inline::Inline;

    struct FailingCollectionRead;

    impl CollectionRead for FailingCollectionRead {
        type RecordsError = std::io::Error;
        type RecordIter<'a> = std::vec::IntoIter<Result<CollectionRecord, Self::RecordsError>>;

        fn records<'a>(&'a self) -> Result<Self::RecordIter<'a>, Self::RecordsError> {
            Err(std::io::Error::other("collection observation failed"))
        }
    }

    #[test]
    fn operation_observation_failure_aborts_projection() {
        let collection = Inline::new([1; 32]);
        let a = Inline::new([2; 32]);
        let b = Inline::new([3; 32]);
        let selectors = BTreeSet::from([CollectionRecordSelector::Operation(WantRequest::merge(
            collection, a, b,
        ))]);

        let error = answered_operations(&FailingCollectionRead, &selectors).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn receipts_project_to_exact_input_only_wants() {
        let collection = Inline::new([1; 32]);
        let target = Inline::new([2; 32]);
        let a = Inline::new([3; 32]);
        let b = Inline::new([4; 32]);
        let result = Inline::new([5; 32]);
        assert_eq!(
            want_request_for_record(CollectionRecord::Merge(CollectionMerge::new(
                collection, b, a, result,
            ))),
            Some(WantRequest::merge(collection, a, b))
        );
        assert_eq!(
            want_request_for_record(CollectionRecord::Derive(CollectionDerive::new(
                target, a, result,
            ))),
            Some(WantRequest::derive(target, a))
        );
    }
}
