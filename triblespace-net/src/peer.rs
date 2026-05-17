//! `Peer<S>`: a store wrapped in distributed network sync.
//!
//! Owns the inner store, spawns the iroh network thread on construction,
//! and exposes the standard storage traits (`BlobStore + BlobStorePut +
//! BranchStore`) with two layers of network behavior built in:
//!
//! - **Reads** auto-call [`refresh`](Peer::refresh), which drains pending
//!   incoming gossip events into the wrapped store and re-publishes any
//!   deltas from external writers (e.g. another process appended to the
//!   same pile file). Mirrors `Pile::refresh` — the explicit method is
//!   available for tight loops, but normal storage use Just Works.
//! - **Writes** delegate to the inner store and then announce blobs to
//!   the DHT and gossip branch updates over the topic mesh, all via the
//!   network thread.
//!
//! Branch-state discovery is gossip-driven: HEAD updates for the
//! team's branches flood the team topic and arrive via the
//! [`NetEvent`] channel; the network thread autonomously walks
//! reachable closures via DHT-routed blob fetches. There are no
//! peer-targeted RPCs on the public surface — peers serve content
//! but don't get asked "what branches do you have." That question
//! is asked of the team, via the topic, not of any individual peer.

use std::collections::HashMap;

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::{BlobEncoding, IntoBlob};
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{
    BlobStore, BlobStoreList, BlobStorePut, BranchStore, PushResult,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;

use crate::channel::NetEvent;
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::RawHash;

pub use crate::host::{PeerConfig, SyncDirection};

/// A store wrapped in distributed network sync.
///
/// See the [module-level docs](self) for the full mental model.
///
/// # Example
///
/// Single-user team-of-one setup against a [`Pile`]: the user is
/// their own team root, and the relay accepts only caps signed by
/// (or chained from) their own key. The `self_cap = [0u8; 32]`
/// sentinel will fail any remote `OP_AUTH` it sends — fine for
/// solo workflows where the peer is purely a server.
///
/// Multi-user setups load `team_root` and `self_cap` from the
/// `TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP` environment variables;
/// see the [Capability Auth] book chapter for the full team
/// lifecycle.
///
/// [`Pile`]: triblespace_core::repo::pile::Pile
/// [Capability Auth]: https://docs.rs/triblespace/latest/triblespace/book/capability-auth/index.html
///
/// ```rust,no_run
/// use std::collections::HashSet;
/// use std::path::Path;
/// use ed25519_dalek::SigningKey;
/// use rand::rngs::OsRng;
/// use triblespace_core::repo::pile::Pile;
/// use triblespace_core::inline::encodings::hash::Blake3;
/// use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
///
/// let key = SigningKey::generate(&mut OsRng);
/// let pile: Pile = Pile::open(Path::new("./team.pile")).unwrap();
/// let peer = Peer::new(pile, key.clone(), PeerConfig {
///     peers: vec![],                       // bootstrap nodes
///     gossip: true,                        // false = serve/pull-only
///     team_root: key.verifying_key(),      // single-user fallback
///     revoked: HashSet::new(),
///     self_cap: [0u8; 32],
///     direction: SyncDirection::Bidirectional,
/// });
/// // From here `peer` is just a `BlobStore + BlobStorePut +
/// // BranchStore` — wrap it in `Repository::new` and use it like
/// // any other triblespace storage.
/// drop(peer);
/// ```
pub struct Peer<S>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    store: S,
    sender: NetSender,
    receiver: NetReceiver,

    /// Baseline blob snapshot for diff-and-publish on `refresh`. The Reader
    /// is a frozen view (for backends with snapshot semantics like Pile) so
    /// `current.blobs_diff(&last)` returns exactly the blobs added since
    /// the last refresh.
    last_blob_reader: Option<S::Reader>,

    /// Baseline branch heads for diff-and-publish on `refresh`. Updated on
    /// every Peer-driven write so we don't double-gossip our own changes.
    last_branches: HashMap<Id, RawHash>,

    /// Direction of swarm participation — controls whether we publish
    /// local HEADs and/or react to remote HEADs.
    direction: SyncDirection,

    /// Wall-clock time of the most recent NetEvent absorbed in
    /// [`refresh`](Peer::refresh). Drives quiescence-based stopping
    /// in long-running sync drivers.
    last_event_at: std::time::Instant,
}

impl<S> Peer<S>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread internally.
    ///
    /// The thread lives for the Peer's lifetime and shuts down when the
    /// Peer drops.
    pub fn new(mut store: S, key: SigningKey, config: PeerConfig) -> Self {
        let direction = config.direction;
        let (sender, receiver) = host::spawn(key, config);

        // Seed the snapshot served by the network thread so peers
        // requesting via the protocol see our current state immediately.
        if let Some(snap) = StoreSnapshot::from_store(&mut store) {
            sender.update_snapshot(snap);
        }

        // Take an initial blob baseline so refresh()'s first call doesn't
        // re-announce every blob we already had on disk.
        let last_blob_reader = store.reader().ok();

        Peer {
            store,
            sender,
            receiver,
            last_blob_reader,
            last_branches: HashMap::new(),
            direction,
            last_event_at: std::time::Instant::now(),
        }
    }

    /// Wall-clock time of the most recent network event absorbed by
    /// [`refresh`](Self::refresh). Useful for quiescence-based stopping:
    /// long-running sync drivers can poll `peer.last_event_at().elapsed()`
    /// and shut down once the swarm goes silent.
    ///
    /// Constructed-at-`Peer::new` initial value, so the first quiescence
    /// window starts at construction rather than at the first event.
    pub fn last_event_at(&self) -> std::time::Instant {
        self.last_event_at
    }

    /// Direction of swarm participation. See [`SyncDirection`].
    pub fn direction(&self) -> SyncDirection {
        self.direction
    }

    /// This peer's network identity (the iroh node id).
    pub fn id(&self) -> EndpointId {
        self.sender.id()
    }

    /// Reconcile this peer with the latest external state.
    ///
    /// Two phases:
    ///
    /// 1. **Drain incoming events** — pulls any pending gossip
    ///    `NetEvent`s from the network thread into the wrapped store
    ///    (creating tracking branches as needed).
    /// 2. **Publish external writes** — diffs the wrapped store against
    ///    the last published baseline and gossips/announces any deltas
    ///    that didn't go through the Peer's own write path. Use this to
    ///    catch writes from another process that touched the pile file.
    ///
    /// Auto-called inside the BlobStore/BranchStore read methods, so
    /// callers using the storage normally don't need to invoke it.
    /// Mirrors `Pile::refresh` — the explicit method is available for
    /// "do it now" semantics or tight loops with no read activity.
    pub fn refresh(&mut self) {
        // ── Phase 1: drain incoming events ────────────────────────────
        // WriteOnly suppresses incoming-event handling: we always
        // drain the channel to keep it from filling, but skip the
        // store mutation. The local node has nothing to learn from
        // the swarm.
        while let Some(event) = self.receiver.try_recv() {
            self.last_event_at = std::time::Instant::now();
            if self.direction == SyncDirection::WriteOnly {
                continue;
            }
            match event {
                NetEvent::Blob(data) => {
                    let bytes: Bytes = data.into();
                    let _ = self.store.put::<UnknownBlob, Bytes>(bytes);
                }
                NetEvent::Head { branch, head, publisher } => {
                    if let Some(remote_id) = Id::new(branch) {
                        if let Some(name) = read_remote_name(&mut self.store, &head) {
                            crate::tracking::ensure_tracking_branch(
                                &mut self.store,
                                remote_id,
                                &head,
                                &name,
                                &publisher,
                            );
                        }
                    }
                }
            }
        }

        // ── Phase 2: diff-and-publish blob deltas ─────────────────────
        // ReadOnly skips the publish: we still update the baseline
        // reader so we don't accumulate a publish backlog if the
        // direction later changes.
        if let Ok(current) = self.store.reader() {
            if let Some(baseline) = self.last_blob_reader.as_ref() {
                if self.direction != SyncDirection::ReadOnly {
                    for handle in current.blobs_diff(baseline).flatten() {
                        self.sender.announce(handle.raw);
                    }
                }
            }
            self.last_blob_reader = Some(current);
        }

        // ── Phase 3: diff-and-publish branch deltas ───────────────────
        // ReadOnly skips this entire phase — followers don't gossip.
        if self.direction != SyncDirection::ReadOnly {
            let bids: Vec<Id> = match self.store.branches() {
                Ok(it) => it.filter_map(|r| r.ok()).collect(),
                Err(_) => return,
            };
            for bid in bids {
                if crate::tracking::is_tracking_branch(&mut self.store, bid) {
                    continue;
                }
                let head = match self.store.head(bid) {
                    Ok(Some(h)) => h,
                    _ => continue,
                };
                if self.last_branches.get(&bid) != Some(&head.raw) {
                    let bid_bytes: [u8; 16] = bid.into();
                    self.sender.gossip(bid_bytes, head.raw);
                    self.last_branches.insert(bid, head.raw);
                }
            }
        }

        // ── Phase 4: refresh the snapshot served by the network thread ─
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
        }
    }

    /// Force-republish all current non-tracking branches to the gossip
    /// topic, regardless of whether they appear changed since the last
    /// publish.
    ///
    /// Use this for periodic "I'm still here, here's my state"
    /// announcements that help newly-joined gossip neighbors learn about
    /// us. Long-running sync daemons typically call this every few seconds.
    /// Cheap to call repeatedly — iroh-gossip dedupes identical messages
    /// on the wire.
    ///
    /// Distinct from [`refresh`](Self::refresh): refresh publishes only
    /// the deltas it detects against its diff baselines. This method
    /// republishes everything unconditionally.
    pub fn republish_branches(&mut self) {
        // ReadOnly suppresses publishing entirely — even republish.
        if self.direction == SyncDirection::ReadOnly {
            return;
        }
        let bids: Vec<Id> = match self.store.branches() {
            Ok(it) => it.filter_map(|r| r.ok()).collect(),
            Err(_) => return,
        };
        for bid in bids {
            if crate::tracking::is_tracking_branch(&mut self.store, bid) {
                continue;
            }
            if let Ok(Some(head)) = self.store.head(bid) {
                let bid_bytes: [u8; 16] = bid.into();
                self.sender.gossip(bid_bytes, head.raw);
                self.last_branches.insert(bid, head.raw);
            }
        }
        // Refresh the snapshot served by the network thread.
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
        }
    }

    /// Borrow the underlying store. Use for store-specific methods that
    /// aren't part of the BlobStore/BranchStore traits (e.g. `Pile::flush`).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Mutably borrow the underlying store. Writes through this borrow
    /// bypass the Peer's auto-publish and become invisible to the network
    /// until the next [`refresh`](Self::refresh) (which is auto-called on
    /// the next read).
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Consume the Peer and return the underlying store. The network
    /// thread shuts down when the Peer drops.
    pub fn into_store(self) -> S {
        self.store
    }
}

// ── Trait delegations ───────────────────────────────────────────────
//
// Reads (`reader`, `head`, `branches`) call `refresh()` first so they
// always see the latest gossiped state AND any external writes that
// landed since the last refresh get announced. Writes (`put`, `update`)
// delegate to the inner store and then push the new state out via the
// network thread, updating the diff baselines so refresh doesn't
// double-announce.

impl<S> BlobStorePut for Peer<S>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        let handle = self.store.put(item)?;
        if self.direction != SyncDirection::ReadOnly {
            self.sender.announce(handle.raw);
        }
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = self.store.reader().ok();
        Ok(handle)
    }
}

impl<S> BlobStore for Peer<S>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    type Reader = S::Reader;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh();
        self.store.reader()
    }
}

impl<S> BranchStore for Peer<S>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    type BranchesError = S::BranchesError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;
    type ListIter<'a> = S::ListIter<'a> where S: 'a;

    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError> {
        self.refresh();
        self.store.branches()
    }

    fn head(
        &mut self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        self.refresh();
        self.store.head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let result = self.store.update(id, old, new.clone())?;
        if let PushResult::Success() = &result {
            if let Some(head) = new {
                // Tracking branches are local mirror state and must NOT be
                // re-gossiped — otherwise the publisher would receive its
                // own tracking branch back and create a tracking-of-the-
                // tracking, ad infinitum.
                if !crate::tracking::is_tracking_branch(&mut self.store, id)
                    && self.direction != SyncDirection::ReadOnly
                {
                    let bid_bytes: [u8; 16] = id.into();
                    self.sender.gossip(bid_bytes, head.raw);
                    self.last_branches.insert(id, head.raw);
                }
                // Refresh the snapshot served by the network thread.
                if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
                    self.sender.update_snapshot(snap);
                }
            }
        }
        Ok(result)
    }
}

/// Read the branch name from a branch metadata blob. Tries `metadata::name`
/// first (normal branches) and falls back to `remote_name` (tracking
/// branches mirrored from a remote peer).
fn read_remote_name<S: BlobStore>(store: &mut S, head_hash: &RawHash) -> Option<String> {
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::macros::{find, pattern};

    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*head_hash);
    let meta: triblespace_core::trible::TribleSet = reader.get(meta_handle).ok()?;

    let name_handle: Inline<Handle<LongString>> = find!(
        h: Inline<Handle<LongString>>,
        pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
    )
    .next()
    .or_else(|| {
        find!(
            h: Inline<Handle<LongString>>,
            pattern!(&meta, [{ _?e @ crate::tracking::remote_name: ?h }])
        )
        .next()
    })?;

    let name_view: anybytes::View<str> = reader.get(name_handle).ok()?;
    Some(name_view.as_ref().to_string())
}
