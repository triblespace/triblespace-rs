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
//! Use [`track`](Peer::track) to start tracking a remote branch from a
//! specific peer (the `pile net pull` workflow), and [`fetch`](Peer::fetch)
//! for single-blob pulls. Set `gossip_topic: None` in [`PeerConfig`] for
//! pull-only mode where the peer doesn't subscribe to a flood mesh.

use std::collections::HashMap;

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::{BlobSchema, ToBlob};
use triblespace_core::blob::schemas::UnknownBlob;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{
    BlobStore, BlobStoreList, BlobStorePut, BranchStore, PushResult,
};
use triblespace_core::value::Value;
use triblespace_core::value::ValueSchema;
use triblespace_core::value::schemas::hash::{Blake3, Handle};

use crate::channel::NetEvent;
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::{RawBranchId, RawHash};

pub use crate::host::PeerConfig;

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
/// use triblespace_core::value::schemas::hash::Blake3;
/// use triblespace_net::peer::{Peer, PeerConfig};
///
/// let key = SigningKey::generate(&mut OsRng);
/// let pile: Pile<Blake3> = Pile::open(Path::new("./team.pile")).unwrap();
/// let peer = Peer::new(pile, key.clone(), PeerConfig {
///     peers: vec![],                       // bootstrap nodes
///     gossip_topic: Some("my-team".into()), // None = serve-only mode
///     team_root: key.verifying_key(),      // single-user fallback
///     revoked: HashSet::new(),
///     self_cap: [0u8; 32],
/// });
/// // From here `peer` is just a `BlobStore + BlobStorePut +
/// // BranchStore` — wrap it in `Repository::new` and use it like
/// // any other triblespace storage.
/// drop(peer);
/// ```
pub struct Peer<S>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
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
}

impl<S> Peer<S>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread internally.
    ///
    /// The thread lives for the Peer's lifetime and shuts down when the
    /// Peer drops.
    pub fn new(mut store: S, key: SigningKey, config: PeerConfig) -> Self {
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
        }
    }

    /// This peer's network identity (the iroh node id).
    pub fn id(&self) -> EndpointId {
        self.sender.id()
    }

    /// Start tracking a remote branch: recursively fetch the blobs
    /// reachable from its head and materialize a local tracking branch.
    ///
    /// Used by `pile net pull` and other "go get this from over there"
    /// workflows. Does not require `gossip_topic` to be set — works in
    /// pull-only mode too. The fetched data lands in the wrapped store
    /// via the same auto-drain path that `refresh` uses.
    ///
    /// Fire-and-forget: returns immediately. Use [`pull_branch`](Self::pull_branch)
    /// if you want to block until the tracking branch is materialized.
    pub fn track(&self, peer: EndpointId, branch: RawBranchId) {
        self.sender.track(peer, branch);
    }

    /// High-level pull: resolve a branch by *name* on a remote peer,
    /// kick off a reachable-closure fetch, and block until the local
    /// tracking branch is materialized. Returns the local tracking
    /// branch id — feed it straight into `Repository::pull` to get a
    /// workspace that merges via `merge_commit`.
    ///
    /// Composes [`list_remote_branches`](Self::list_remote_branches),
    /// [`fetch`](Self::fetch), [`track`](Self::track), and the tracking
    /// auto-drain on reads. Times out at 30 s if the remote never sends
    /// the HEAD.
    pub fn pull_branch(
        &mut self,
        remote: EndpointId,
        name: &str,
    ) -> anyhow::Result<Id> {
        let (remote_id, _head) = resolve_branch_name(self, remote, name)?
            .ok_or_else(|| anyhow::anyhow!("branch '{name}' not found on remote"))?;

        let branch_bytes: [u8; 16] = remote_id.into();
        self.track(remote, branch_bytes);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Some(id) = crate::tracking::find_tracking_branch(self, remote_id) {
                return Ok(id);
            }
            if std::time::Instant::now() > deadline {
                return Err(anyhow::anyhow!("timed out waiting for remote HEAD"));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// RPC: list a remote peer's branches. One protocol round trip.
    ///
    /// Primitive for building branch-discovery workflows (e.g. resolving
    /// a branch name to its ID before calling [`track`](Self::track)).
    pub fn list_remote_branches(
        &self,
        peer: EndpointId,
    ) -> anyhow::Result<Vec<(Id, RawHash)>> {
        self.sender.list_remote_branches(peer)
    }

    /// RPC: query a remote peer for its current head of one branch.
    /// One protocol round trip.
    pub fn head_of_remote(
        &mut self,
        peer: EndpointId,
        branch: RawBranchId,
    ) -> anyhow::Result<Option<RawHash>> {
        self.sender.head_of_remote(peer, branch)
    }

    /// RPC: fetch a single blob from a remote peer, insert it into the
    /// local store, and decode it to `T`. Returns `None` if the remote
    /// didn't have it.
    ///
    /// Mirrors [`BlobStoreGet::get`][bsg] in shape — pass a typed handle,
    /// pick what you want out the other side. Request `Blob<Sch>` for
    /// "just the bytes" with zero decode cost; request `TribleSet`,
    /// `anybytes::View<str>`, etc. for the decoded value.
    ///
    /// Unlike [`track`](Self::track), this is a single blob (no
    /// reachable closure traversal) and blocks until the round trip
    /// completes.
    ///
    /// [bsg]: triblespace_core::repo::BlobStoreGet::get
    pub fn fetch<T, Sch>(
        &mut self,
        peer: EndpointId,
        handle: Value<Handle<Blake3, Sch>>,
    ) -> anyhow::Result<Option<T>>
    where
        Sch: BlobSchema + 'static,
        T: triblespace_core::blob::TryFromBlob<Sch>,
        Handle<Blake3, Sch>: ValueSchema,
    {
        let Some(bytes) = self.sender.fetch(peer, handle.raw)? else {
            return Ok(None);
        };
        let data: Bytes = bytes.into();
        // Persist locally under UnknownBlob — blobs are keyed by raw
        // hash, so the schema tag at put time doesn't affect what you
        // can get back out.
        self.store
            .put::<UnknownBlob, Bytes>(data.clone())
            .map_err(|_| anyhow::anyhow!("store put failed"))?;
        // Keep the blob diff baseline current so refresh doesn't
        // re-announce this blob we just pulled.
        self.last_blob_reader = self.store.reader().ok();

        // Decode directly from the bytes we already have in hand — no
        // second trip through the store needed.
        let blob: triblespace_core::blob::Blob<Sch> =
            triblespace_core::blob::Blob::new(data);
        T::try_from_blob(blob)
            .map(Some)
            .map_err(|_| anyhow::anyhow!("blob decode failed"))
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
        while let Some(event) = self.receiver.try_recv() {
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
        if let Ok(current) = self.store.reader() {
            if let Some(baseline) = self.last_blob_reader.as_ref() {
                for handle in current.blobs_diff(baseline).flatten() {
                    self.sender.announce(handle.raw);
                }
            }
            self.last_blob_reader = Some(current);
        }

        // ── Phase 3: diff-and-publish branch deltas ───────────────────
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

impl<S> BlobStorePut<Blake3> for Peer<S>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Value<Handle<Blake3, Sch>>, Self::PutError>
    where
        Sch: BlobSchema + 'static,
        T: ToBlob<Sch>,
        Handle<Blake3, Sch>: ValueSchema,
    {
        let handle = self.store.put(item)?;
        self.sender.announce(handle.raw);
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = self.store.reader().ok();
        Ok(handle)
    }
}

impl<S> BlobStore<Blake3> for Peer<S>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
{
    type Reader = S::Reader;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh();
        self.store.reader()
    }
}

impl<S> BranchStore<Blake3> for Peer<S>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
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
    ) -> Result<Option<Value<Handle<Blake3, SimpleArchive>>>, Self::HeadError> {
        self.refresh();
        self.store.head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<Blake3, SimpleArchive>>>,
        new: Option<Value<Handle<Blake3, SimpleArchive>>>,
    ) -> Result<PushResult<Blake3>, Self::UpdateError> {
        let result = self.store.update(id, old, new.clone())?;
        if let PushResult::Success() = &result {
            if let Some(head) = new {
                // Tracking branches are local mirror state and must NOT be
                // re-gossiped — otherwise the publisher would receive its
                // own tracking branch back and create a tracking-of-the-
                // tracking, ad infinitum.
                if !crate::tracking::is_tracking_branch(&mut self.store, id) {
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

/// Resolve a branch *name* on a remote peer to its `(Id, head)`.
///
/// Composes [`Peer::list_remote_branches`] and [`Peer::fetch`]:
/// lists the remote's branches, pulls each metadata blob into the local
/// store, queries for `metadata::name`, fetches the name string blob, and
/// matches against the requested name. Returns `Ok(None)` if no branch
/// matches.
///
/// This is the name-lookup half of the `pile net pull` workflow — the
/// caller then hands the resolved `Id` to [`Peer::track`] to pull the
/// branch's reachable blob closure.
pub fn resolve_branch_name<S>(
    peer: &mut Peer<S>,
    remote: EndpointId,
    name: &str,
) -> anyhow::Result<Option<(Id, RawHash)>>
where
    S: BlobStore<Blake3> + BlobStorePut<Blake3> + BranchStore<Blake3>,
{
    use triblespace_core::blob::schemas::longstring::LongString;
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::trible::TribleSet;

    let branches = peer.list_remote_branches(remote)?;
    for (id, head) in branches {
        let meta_handle = Value::<Handle<Blake3, SimpleArchive>>::new(head);
        let Some(meta) = peer.fetch::<TribleSet, _>(remote, meta_handle)? else {
            continue;
        };

        let name_handles: Vec<Value<Handle<Blake3, LongString>>> = find!(
            h: Value<Handle<Blake3, LongString>>,
            pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
        )
        .collect();

        for name_handle in name_handles {
            let Some(name_view) = peer.fetch::<anybytes::View<str>, _>(remote, name_handle)? else {
                continue;
            };
            if name_view.as_ref() == name {
                return Ok(Some((id, head)));
            }
        }
    }
    Ok(None)
}

/// Read the branch name from a branch metadata blob. Tries `metadata::name`
/// first (normal branches) and falls back to `remote_name` (tracking
/// branches mirrored from a remote peer).
fn read_remote_name<S: BlobStore<Blake3>>(store: &mut S, head_hash: &RawHash) -> Option<String> {
    use triblespace_core::blob::schemas::longstring::LongString;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::macros::{find, pattern};

    let reader = store.reader().ok()?;
    let meta_handle = Value::<Handle<Blake3, SimpleArchive>>::new(*head_hash);
    let meta: triblespace_core::trible::TribleSet = reader.get(meta_handle).ok()?;

    let name_handle: Value<Handle<Blake3, LongString>> = find!(
        h: Value<Handle<Blake3, LongString>>,
        pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
    )
    .next()
    .or_else(|| {
        find!(
            h: Value<Handle<Blake3, LongString>>,
            pattern!(&meta, [{ _?e @ crate::tracking::remote_name: ?h }])
        )
        .next()
    })?;

    let name_view: anybytes::View<str> = reader.get(name_handle).ok()?;
    Some(name_view.as_ref().to_string())
}
