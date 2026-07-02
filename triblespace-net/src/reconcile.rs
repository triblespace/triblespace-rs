//! Want-reconcile: service durable weak-pin **wants** by fetching the
//! absent blobs from the swarm.
//!
//! A weak pin IS a durable want-marker — "I would like this blob; fetch
//! it if absent; evictable." Faculties and other processes append
//! weak-pin records to the shared pile out-of-band; a long-running sync
//! daemon (`trible pile net sync`) services that queue. This module is
//! the mechanism, the CLI is just the wiring: each
//! [`tick`](Reconciler::tick) diffs the LWW-resolved weak-pin set
//! against the blobs actually present, drives the [`Peer`]'s existing
//! swarm fetch for each missing want, and keeps per-want retry state
//! (exponential backoff) for the ones nobody served yet.
//!
//! Semantics, per the retention lattice `pin ⊐ weak-pin ⊐ weak-unpin ⊐
//! unpin` (LWW by log position):
//!
//! - **Strong pins and branches are never touched.** The reconciler
//!   reads weak pins and lands blobs; it records no pin state of any
//!   kind (the weak pin that expressed the want is already on record
//!   and becomes the retention marker for the landed blob).
//! - **"Absent" is always "not obtained yet", never definitely-absent**
//!   — existence is semidecidable. A want that can't be satisfied stays
//!   pending and is retried with backoff; it is NOT an error and NOT
//!   dropped. The weak pin stays on record until the blob lands or
//!   someone weak-unpins it.
//! - A weak pin whose blob is already present is a retention marker,
//!   not an outstanding want — the presence diff filters it out.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anybytes::Bytes;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::inline::Inline;
use triblespace_core::repo::{
    BlobStore, BlobStoreGet, BlobStorePut, PinStore, StorageFlush, WeakPinStore,
};

use crate::peer::Peer;
use crate::protocol::RawHash;

/// Counters from one reconcile pass — the observable a sync daemon
/// surfaces (trace lines, `--quiescent-for` scripts) so lazy progress
/// is legible from the outside.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Weak-pinned handles seen this pass — the full LWW-resolved want
    /// set, whether the blob is present or not.
    pub wants: usize,
    /// Wants whose blob was absent locally at the start of the pass.
    pub missing: usize,
    /// Fetches actually issued this pass (`missing` minus the
    /// backoff-gated).
    pub attempted: usize,
    /// Wants satisfied this pass: fetched from the swarm and landed in
    /// the store.
    pub fetched: usize,
    /// Wants still outstanding after the pass. **Normal, not an error**
    /// — they stay on record (the weak pin) and are retried with
    /// backoff on later passes.
    pub pending: usize,
}

/// Per-want retry bookkeeping. A state exists only while the want is
/// outstanding *and* has failed at least once — a want satisfied on its
/// first attempt never allocates one. In-memory only: it rebuilds
/// naturally (first retry immediate) if the daemon restarts, while the
/// wants themselves live durably in the store as weak pins.
struct WantState {
    /// When the last fetch attempt resolved (Unavailable). Read through
    /// [`crate::clock`] so simulated runs back off in virtual time.
    last_attempt: crate::clock::Mono,
    /// Current retry delay; doubles per failure up to the cap.
    backoff: Duration,
}

/// Drives the want-reconcile loop over a [`Peer`]. Owns only the retry
/// bookkeeping — the wants themselves are the store's durable weak
/// pins, so dropping/recreating a `Reconciler` loses nothing but the
/// backoff timers.
pub struct Reconciler {
    states: HashMap<RawHash, WantState>,
    initial_backoff: Duration,
    max_backoff: Duration,
    /// End-to-end budget per fetch attempt. Background work, so more
    /// generous than the interactive default
    /// ([`crate::host::INTERACTIVE_FETCH_DEADLINE`]) — nobody is
    /// blocked on a reconcile tick, and a slow multi-provider walk is
    /// worth finishing. Still bounded: an expired budget resolves
    /// Unavailable and the want retries with backoff on a later pass.
    fetch_budget: Duration,
}

/// Default per-fetch budget for background reconcile ticks.
pub const RECONCILE_FETCH_DEADLINE: Duration = Duration::from_secs(30);

impl Default for Reconciler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reconciler {
    /// Default backoff: first retry ~1s after a failed attempt,
    /// doubling per failure to a 60s cap. Per-fetch budget defaults to
    /// [`RECONCILE_FETCH_DEADLINE`].
    pub fn new() -> Self {
        Self::with_backoff(Duration::from_secs(1), Duration::from_secs(60))
    }

    /// Custom backoff bounds — `initial` after the first failure,
    /// doubling to at most `max`.
    pub fn with_backoff(initial: Duration, max: Duration) -> Self {
        Self {
            states: HashMap::new(),
            initial_backoff: initial,
            max_backoff: max,
            fetch_budget: RECONCILE_FETCH_DEADLINE,
        }
    }

    /// Override the end-to-end budget each fetch attempt gets.
    pub fn with_fetch_budget(mut self, budget: Duration) -> Self {
        self.fetch_budget = budget;
        self
    }

    /// One reconcile pass.
    ///
    /// 1. Enumerate the wants: `weak_pins()` on the wrapped store. The
    ///    store refreshes itself first (a `Pile` re-scans the file), so
    ///    weak-pin records appended by OTHER processes since the last
    ///    tick become visible here.
    /// 2. Diff against presence: take a reader (which also runs
    ///    [`Peer::refresh`] — freshly-gossiped blobs count as present)
    ///    and keep the wants whose blob the local snapshot can't serve.
    /// 3. For each missing want not gated by its backoff timer, drive
    ///    the Peer's swarm fetch and land the verified bytes in the
    ///    store. Failures back off exponentially and are logged once
    ///    per state change (want became pending / pending want
    ///    resolved), not per retry.
    ///
    /// A pass with unsatisfiable wants completes in bounded time (the
    /// fetch resolves Unavailable on the DHT deadline); the wants stay
    /// pending — that is their normal state until a holder is
    /// reachable, never an error.
    pub async fn tick<S>(&mut self, peer: &mut Peer<S>) -> ReconcileStats
    where
        S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
    {
        let mut stats = ReconcileStats::default();

        // ── Wants: the LWW-resolved weak-pin set ──────────────────────
        let wants: Vec<RawHash> = {
            let mut store = peer.store();
            match store.weak_pins() {
                Ok(iter) => iter.filter_map(Result::ok).map(|h| h.raw).collect(),
                Err(e) => {
                    tracing::warn!(error = ?e, "reconcile: weak_pins enumeration failed; skipping pass");
                    return stats;
                }
            }
        };
        stats.wants = wants.len();

        // ── Presence: which wants the local snapshot already serves ───
        // Peer::reader() runs refresh() (drains gossip, announces
        // external writes) and hands back a frozen local snapshot; the
        // sync get on it is local-only by design.
        let reader = match peer.reader() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = ?e, "reconcile: store reader unavailable; skipping pass");
                return stats;
            }
        };
        let missing: Vec<RawHash> = wants
            .into_iter()
            .filter(|hash| {
                BlobStoreGet::get::<Bytes, UnknownBlob>(&reader, Inline::new(*hash)).is_err()
            })
            .collect();
        stats.missing = missing.len();

        // Drop bookkeeping for wants no longer outstanding — satisfied
        // out-of-band (gossip landed the blob) or weak-unpinned.
        let missing_set: HashSet<RawHash> = missing.iter().copied().collect();
        self.states.retain(|hash, _| {
            let keep = missing_set.contains(hash);
            if !keep {
                tracing::info!(
                    hash = %hex::encode(&hash[..4]),
                    "reconcile: pending want resolved out-of-band"
                );
            }
            keep
        });

        // ── Fetch the missing wants (backoff-gated) ───────────────────
        for hash in missing {
            if let Some(st) = self.states.get(&hash) {
                if crate::clock::mono_now().duration_since(st.last_attempt) < st.backoff {
                    // Recently failed; wait out the backoff. Still a
                    // pending want — just not this pass's problem.
                    stats.pending += 1;
                    continue;
                }
            }

            stats.attempted += 1;
            match peer.fetch_blob_with_deadline(hash, self.fetch_budget).await {
                Some(bytes) => {
                    // Land the verified bytes (fetch_blob hash-checked
                    // them). The weak pin that expressed the want is
                    // already on record and now retains the blob — no
                    // pin state changes here.
                    if let Err(e) = peer.store().put::<UnknownBlob, Bytes>(Bytes::from(bytes)) {
                        tracing::warn!(
                            hash = %hex::encode(&hash[..4]),
                            error = ?e,
                            "reconcile: landing fetched blob failed; want stays pending"
                        );
                        stats.pending += 1;
                        continue;
                    }
                    if self.states.remove(&hash).is_some() {
                        // State change: a want previously logged as
                        // pending has been satisfied.
                        tracing::info!(
                            hash = %hex::encode(&hash[..4]),
                            "reconcile: pending want fetched"
                        );
                    } else {
                        tracing::debug!(
                            hash = %hex::encode(&hash[..4]),
                            "reconcile: want fetched"
                        );
                    }
                    stats.fetched += 1;
                }
                None => {
                    // Unavailable: nobody reachable served it. Normal —
                    // the want stays on record (the weak pin), retried
                    // with backoff. Log once on the state change
                    // (became pending), not per retry.
                    stats.pending += 1;
                    let now = crate::clock::mono_now();
                    match self.states.entry(hash) {
                        Entry::Occupied(mut e) => {
                            let st = e.get_mut();
                            st.last_attempt = now;
                            st.backoff = (st.backoff * 2).min(self.max_backoff);
                        }
                        Entry::Vacant(e) => {
                            tracing::info!(
                                hash = %hex::encode(&hash[..4]),
                                "reconcile: want unavailable; pending (retried with backoff — not an error)"
                            );
                            e.insert(WantState {
                                last_attempt: now,
                                backoff: self.initial_backoff,
                            });
                        }
                    }
                }
            }
        }

        stats
    }
}
