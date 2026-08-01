//! Network thread: spawns iroh endpoint, gossip, DHT, protocol server.
//!
//! Private implementation detail of [`crate::peer::Peer`] — `spawn()`
//! returns the [`NetSender`] / [`NetReceiver`] pair the Peer uses to
//! communicate with the async world (commands + snapshot updates one
//! way, events the other).
//!
//! Async is jailed inside the spawned thread.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use ed25519_dalek::SigningKey;
use iroh_base::{EndpointAddr, EndpointId};
use tracing::{Instrument, debug, debug_span, error, info, info_span, instrument, trace, warn};

use crate::channel::{NetCommand, NetEvent, PublisherKey};
use crate::identity::iroh_secret;
use crate::protocol::*;
use crate::transport::{Conn, GossipEvent, GossipSink, Harness, PeerId, Transport};
use tokio::io::AsyncWriteExt;

fn op_name(op: u8) -> &'static str {
    match op {
        OP_AUTH => "AUTH",
        OP_GET_BLOB => "GET_BLOB",
        OP_CHILDREN => "CHILDREN",
        _ => "UNKNOWN",
    }
}

/// Builds a [`RelayMap`] mirroring iroh's prod default but with
/// trailing dots stripped from each relay's hostname.
///
/// Iroh's `iroh::defaults::prod` ships FQDN-absolute hostnames
/// (e.g. `"euc1-1.relay.n0.iroh-canary.iroh.link."` — note the
/// trailing dot, which is the DNS-absolute marker). When iroh
/// constructs HTTPS probe URLs via `Url::parse(...)`, the dot
/// rides through into reqwest's `Host` header. WAFs that treat
/// trailing-dot Host as a known bypass-attempt signature
/// (Anthropic's web-sandbox egress proxy is one) reject those
/// requests with synthetic 503s, which permanently jams iroh's
/// `net_report` cycle and prevents any relay session — and,
/// in iroh's current connect-path design, prevents direct-dial
/// attempts that would otherwise honor a ticket's pre-known
/// addresses.
///
/// Stripping the trailing dot before iroh constructs its
/// `RelayUrl`s produces an HTTP-canonical Host header that the
/// WAFs pass through unmolested. Resolves to the same upstream
/// relay (DNS resolution doesn't care about the absolute/relative
/// distinction); just a different on-the-wire request shape.
///
/// We transform the upstream default rather than hardcoding
/// hostnames, so we stay in sync with whatever n0 ships in
/// `iroh::defaults::prod::default_relay_map()`.
pub(crate) fn dot_stripped_default_relay_map() -> iroh::RelayMap {
    let original = iroh::defaults::prod::default_relay_map();
    let stripped_urls: Vec<String> = original
        .urls::<Vec<_>>()
        .into_iter()
        .map(|relay_url| {
            let mut url: url::Url = relay_url.into();
            if let Some(host) = url.host_str() {
                if let Some(trimmed) = host.strip_suffix('.') {
                    // `set_host` re-validates; on failure (which
                    // shouldn't happen for a valid relay URL with
                    // a trimmable host) we keep the original.
                    let trimmed = trimmed.to_string();
                    let _ = url.set_host(Some(&trimmed));
                }
            }
            url.to_string()
        })
        .collect();
    iroh::RelayMap::try_from_iter(stripped_urls.iter().map(|s| s.as_str()))
        .expect("stripped relay URLs are valid (transformed from valid input)")
}

/// Configuration for [`Peer::new`](crate::peer::Peer::new). No
/// `Default` impl — auth is mandatory in protocol v4 so every peer
/// construction site must explicitly choose a team root. For solo
/// workflows the convention is `team_root = signing_key.verifying_key()`
/// (the user is the team root and the founder of a team-of-one);
/// see the `Peer` struct's doctest for the full pattern.
pub struct PeerConfig {
    /// Bootstrap peers — for both the gossip mesh and the DHT.
    /// `EndpointAddr` here carries only an `EndpointId`; iroh's
    /// standard discovery (pkarr / DNS via `presets::N0`) resolves
    /// the actual relay URL and direct addresses at dial time.
    pub peers: Vec<EndpointAddr>,
    /// Whether to subscribe to legacy mutable-HEAD gossip. The topic id
    /// is the team root pubkey's 32 bytes — every team has exactly
    /// one gossip mesh, derived from its identity. `false` serves only the
    /// authenticated blob RPCs (no subscription or broadcasts).
    pub gossip: bool,
    /// The team root public key — verifies all incoming capability
    /// chains. Every connection's first stream must present a cap that
    /// chains back to this key. See `triblespace_core::repo::capability`.
    /// When `gossip = true`, also serves as the gossip topic id.
    pub team_root: ed25519_dalek::VerifyingKey,
    /// This node's own capability sig handle. Presented to remote peers
    /// as the first stream on every outgoing connection so they can
    /// authorise us. Required — protocol v4 has mandatory auth on both
    /// directions of a connection.
    pub self_cap: RawHash,
    /// Direction of participation in the team swarm. Controls whether
    /// this node publishes eligible legacy mutable HEADs (write side) and/or
    /// reacts to incoming observations (read side). Default is
    /// `Bidirectional`. Use [`SyncDirection::ReadOnly`] for follower /
    /// catch-up workflows; use [`SyncDirection::WriteOnly`] for
    /// pure-publisher workflows where the local node has nothing to
    /// learn from the swarm.
    pub direction: SyncDirection,
}

/// Which directions of the team swarm this node participates in.
///
/// The wire protocol is symmetric — every peer runs the same code path
/// — but locally we can choose to suppress one side of the data flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncDirection {
    /// Subscribe to legacy HEAD gossip, follow bounded child-hint walks, and
    /// publish eligible local mutable heads. Default behavior.
    #[default]
    Bidirectional,
    /// Subscribe to gossip + follow bounded child-hint walks, but suppress
    /// local legacy HEAD publishes. Useful for follower / leecher workflows
    /// where the local node is catching up to the swarm and has
    /// no legacy mutable state to contribute.
    ReadOnly,
    /// Publish eligible local mutable heads to gossip, but ignore incoming HEAD
    /// events from peers. Useful for pure-publisher workflows
    /// (e.g. an importer feeding the swarm) where the local node
    /// has nothing to learn from the swarm.
    WriteOnly,
}

// No `Default` impl: every PeerConfig must specify a team root because
// auth is mandatory in protocol v4. For a single-user OSS deployment
// the convention is `team_root = signing_key.verifying_key()` (the user
// is the team root and the founder of a team-of-one).

/// Snapshot of store state for serving protocol requests.
pub struct StoreSnapshot<R> {
    pub reader: R,
    pub pins: triblespace_core::repo::PinSnapshot,
}

impl StoreSnapshot<()> {
    pub fn from_store<S>(store: &mut S) -> Option<StoreSnapshot<S::Reader>>
    where
        S: triblespace_core::repo::BlobStore + triblespace_core::repo::PinStore,
    {
        let pins = store.pin_snapshot().ok()?;
        let reader = store.reader().ok()?;
        Some(StoreSnapshot { reader, pins })
    }
}

/// Type-erased snapshot for the host thread.
///
/// Carries just enough of the pile for the network thread to serve
/// peer requests: per-hash blob fetch, legacy pin-root scope checks, and a
/// quick presence check.
pub trait AnySnapshot: Send + 'static {
    fn get_blob(&self, hash: &RawHash) -> Option<Vec<u8>>;
    fn has_blob(&self, hash: &RawHash) -> bool;
    fn pins(&self) -> &triblespace_core::repo::PinSnapshot;
}

impl<R> AnySnapshot for StoreSnapshot<R>
where
    R: triblespace_core::repo::BlobStoreGet
        + triblespace_core::repo::BlobStoreList
        + Send
        + 'static,
{
    fn get_blob(&self, hash: &RawHash) -> Option<Vec<u8>> {
        use triblespace_core::blob::encodings::UnknownBlob;
        use triblespace_core::inline::Inline;
        use triblespace_core::inline::encodings::hash::Handle;
        let handle = Inline::<Handle<UnknownBlob>>::new(*hash);
        self.reader
            .get::<anybytes::Bytes, UnknownBlob>(handle)
            .ok()
            .map(|b| b.to_vec())
    }

    fn has_blob(&self, hash: &RawHash) -> bool {
        self.get_blob(hash).is_some()
    }

    fn pins(&self) -> &triblespace_core::repo::PinSnapshot {
        &self.pins
    }
}

/// The network capability a `Peer` invokes **inline** for
/// request/response work — currently the lazy read-miss swarm fetch.
///
/// This is what replaces the old `FetchBlob` command round-trip: rather
/// than ship a command to the host loop and await a reply channel, the
/// Peer method awaits this directly and the fetch runs in its own task.
/// Type-erased over the transport so `Peer` stays transport-agnostic;
/// published through a readiness slot ([`NetSender::fetch_blob`]) once
/// the transport binds, which is how the inline path handles the
/// construction-ordering the command channel used to paper over.
pub trait NetCapability: Send + Sync {
    /// Swarm-addressed fetch of `hash` (DHT-routed, content-verified).
    /// `None` is Unavailable.
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Vec<u8>>>;
}

/// Gossip-suggested provider endpoints, most-recent-first. A legacy HEAD frame
/// carries a routing hint, so recent hints are a useful bounded first guess for
/// an on-demand fetch before paying for a DHT lookup. They carry no authorship
/// authority. Vec, not a hash set:
/// insertion order is event order, which keeps deterministic simulation
/// replay intact.
type KnownPublishers = Arc<Mutex<Vec<PeerId>>>;

/// Normalize the unauthenticated routing field in a legacy gossip frame.
/// Invalid key bytes cannot name a tracking namespace, so use the
/// transport-authenticated relaying peer consistently for routing, retry
/// arbitration, and the eventual tracking event.
fn normalize_publisher(claimed: PublisherKey, delivered_from: PeerId) -> PublisherKey {
    if ed25519_dalek::VerifyingKey::from_bytes(&claimed).is_ok() {
        claimed
    } else {
        delivered_from
    }
}

/// Per-fetch bound on candidate providers. Team meshes are small; eight
/// remembered publishers or ordered DHT fallbacks covers them while bounding
/// deduplication work and worst-case dial fanout (each attempt is further
/// bounded by the caller's overall fetch budget).
const PROVIDER_FANOUT_CAP: usize = 8;

/// Move `peer` to the front of the known-publisher list (dedup + cap).
fn note_publisher(known: &KnownPublishers, peer: PeerId) {
    let mut list = known.lock().unwrap();
    if let Some(pos) = list.iter().position(|p| *p == peer) {
        list.remove(pos);
    }
    list.insert(0, peer);
    list.truncate(PROVIDER_FANOUT_CAP);
}

/// Transport-bound implementation of [`NetCapability`]. Holds exactly
/// what the fetch needs; built in the host once the transport exists.
struct NetCap<T: Transport> {
    transport: T,
    pool: SharedPool<T::Conn>,
    self_cap: RawHash,
    my_id: PeerId,
    /// Gossip-suggested provider endpoints — consulted before the DHT on every
    /// on-demand fetch. The list is only a routing optimization: returned bytes
    /// remain content-verified, and the DHT is the fallback.
    publishers: KnownPublishers,
}

impl<T: Transport> NetCapability for NetCap<T> {
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Vec<u8>>> {
        let t = self.transport.clone();
        let pool = self.pool.clone();
        let self_cap = self.self_cap;
        let my_id = self.my_id;
        // Snapshot the publisher list now (sync lock, most-recent-first,
        // self excluded) so the future is self-contained.
        let known: Vec<PeerId> = self
            .publishers
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|p| *p != my_id)
            .collect();
        Box::pin(async move {
            // Publisher-first: whoever gossiped a HEAD at us is a live,
            // dialable holder candidate — ask them before the DHT.
            let mut data = if known.is_empty() {
                None
            } else {
                fetch_from_providers(&t, &hash, &pool, &known, &self_cap).await
            };
            // DHT fallback: no publisher known, or none of them held it.
            if data.is_none() {
                data = fetch_one(&t, &hash, &pool, my_id, &self_cap).await;
            }
            data
        })
    }
}

// ── Outgoing half ────────────────────────────────────────────────────

/// Default overall budget for an **interactive** on-demand blob fetch
/// (a lazy read a caller is actively waiting on). Bounds the WHOLE
/// resolution — capability readiness, DHT lookup, every per-provider
/// dial + op — where the per-stage deadlines alone (`DIAL_DEADLINE`,
/// `OP_DEADLINE`) could stack up to 40s+ across a provider list.
/// Background work (the want-reconciler) passes its own, more generous
/// budget; the want stays durably recorded either way, so an expired
/// budget only defers the fetch, never loses the demand.
pub const INTERACTIVE_FETCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Send fire-and-forget commands to the host loop, refresh the serving
/// snapshot, and invoke inline request/response capabilities (the swarm
/// fetch). `update_snapshot` is a pure snapshot refresh; `fetch_blob`
/// awaits the inline capability rather than the command loop.
#[derive(Clone)]
pub struct NetSender {
    cmd_tx: mpsc::Sender<NetCommand>,
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Readiness slot for the inline fetch capability, published by the
    /// host once its transport binds. `None` until then.
    cap: tokio::sync::watch::Receiver<Option<Arc<dyn NetCapability>>>,
    id: EndpointId,
}

impl NetSender {
    pub fn id(&self) -> EndpointId {
        self.id
    }

    pub fn announce(&self, hash: RawHash) {
        let _ = self.cmd_tx.send(NetCommand::Announce(hash));
    }

    pub fn gossip_legacy_head(&self, pin: RawPinId, metadata_head: RawHash) {
        let _ = self
            .cmd_tx
            .send(NetCommand::GossipLegacyHead { pin, metadata_head });
    }

    /// Dispatch a freshly-signed (cap, sig) blob pair to `subject`.
    /// Fire-and-forget — the network thread handles the dial,
    /// `OP_DELIVER_CAP`, and connection teardown. Used by the
    /// renewal daemon and `team approve`.
    pub fn deliver_cap(
        &self,
        subject: PublisherKey,
        cap_bytes: anybytes::Bytes,
        sig_bytes: anybytes::Bytes,
    ) {
        let _ = self.cmd_tx.send(NetCommand::DeliverCap {
            subject,
            cap_bytes,
            sig_bytes,
        });
    }

    pub fn update_snapshot(&self, snapshot: impl AnySnapshot) {
        let boxed: Box<dyn AnySnapshot> = Box::new(snapshot);
        *self.snapshot.lock().unwrap() = Some(boxed);
    }

    /// Swarm-addressed on-demand blob fetch (lazy read-miss) — run
    /// **inline**, not via the command loop. Awaits the network
    /// capability becoming ready (published once the host's transport
    /// binds), then runs the fetch in this task. `None` is Unavailable:
    /// no provider served it, the host never came up, or `budget`
    /// expired.
    ///
    /// `budget` is the END-TO-END deadline over the whole resolution
    /// (capability readiness + DHT lookup + every provider attempt).
    /// Interactive callers pass [`INTERACTIVE_FETCH_DEADLINE`];
    /// background reconcile ticks pass a longer one. Expiry has the
    /// same semantics as any other Unavailable — a recorded want stays
    /// recorded.
    pub async fn fetch_blob(&self, hash: RawHash, budget: std::time::Duration) -> Option<Vec<u8>> {
        match tokio::time::timeout(budget, self.fetch_blob_unbounded(hash)).await {
            Ok(result) => result,
            Err(_) => {
                debug!(
                    hash = %hex::encode(&hash[..4]),
                    budget = ?budget,
                    "fetch_blob: overall budget exceeded; Unavailable"
                );
                None
            }
        }
    }

    /// The unbounded fetch [`fetch_blob`](Self::fetch_blob) wraps in its
    /// overall budget. Kept private: every public path must carry an
    /// end-to-end deadline (per-stage deadlines alone can stack to 40s+
    /// across a provider list).
    async fn fetch_blob_unbounded(&self, hash: RawHash) -> Option<Vec<u8>> {
        let mut rx = self.cap.clone();
        // Resolve the capability — immediate if already published, else
        // park until the transport binds. `Err` means the host dropped
        // its sender (gone) → Unavailable.
        let cap = match rx.wait_for(|c| c.is_some()).await {
            Ok(guard) => guard.clone(),
            Err(_) => return None,
        };
        match cap {
            Some(cap) => cap.fetch_blob(hash).await,
            None => None,
        }
    }
}

// ── Incoming half ────────────────────────────────────────────────────

/// Receive events from the network thread.
pub struct NetReceiver {
    evt_rx: mpsc::Receiver<NetEvent>,
}

impl NetReceiver {
    pub fn try_recv(&self) -> Option<NetEvent> {
        self.evt_rx.try_recv().ok()
    }
}

// ── Spawn ────────────────────────────────────────────────────────────

/// The host loop's end of the Peer↔host channel pair, plus the shared
/// serving-snapshot slot. Produced by [`wire`]; consumed by
/// [`run_host`]. Exists so the loop can run either on its own thread
/// + runtime (production, [`spawn`]) or as a task on a caller-owned
/// runtime (deterministic simulation, where every node shares one
/// paused current-thread runtime).
pub struct HostWiring {
    pub(crate) cmd_rx: mpsc::Receiver<NetCommand>,
    pub(crate) evt_tx: mpsc::Sender<NetEvent>,
    pub(crate) snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Publish half of the inline-fetch capability slot; the host fills
    /// it once its transport binds.
    pub(crate) cap_tx: tokio::sync::watch::Sender<Option<Arc<dyn NetCapability>>>,
}

/// Build the Peer↔host channel pair for a node with identity `id`.
/// The `(NetSender, NetReceiver)` half goes to the Peer; the
/// [`HostWiring`] half goes to [`run_host`].
pub fn wire(id: EndpointId) -> (NetSender, NetReceiver, HostWiring) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<NetCommand>();
    let (evt_tx, evt_rx) = mpsc::channel::<NetEvent>();
    let snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>> = Arc::new(Mutex::new(None));
    let (cap_tx, cap_rx) = tokio::sync::watch::channel::<Option<Arc<dyn NetCapability>>>(None);

    let sender = NetSender {
        cmd_tx,
        snapshot: snapshot.clone(),
        cap: cap_rx,
        id,
    };
    let receiver = NetReceiver { evt_rx };
    let wiring = HostWiring {
        cmd_rx,
        evt_tx,
        snapshot,
        cap_tx,
    };
    (sender, receiver, wiring)
}

/// Run the host loop over an already-constructed transport harness.
/// This is the transport-generic entry point: production wraps it in
/// a dedicated thread ([`spawn`]); the simulator spawns it as a local
/// task per node on one shared deterministic runtime.
pub async fn run_host<T: Transport>(harness: Harness<T>, config: PeerConfig, wiring: HostWiring) {
    host_loop(
        harness,
        config,
        wiring.cmd_rx,
        wiring.evt_tx,
        wiring.snapshot,
        wiring.cap_tx,
    )
    .await;
}

/// Spawn the network thread. Returns the outgoing/incoming channel halves
/// — used internally by [`Peer::new`](crate::peer::Peer::new).
pub fn spawn(key: SigningKey, config: PeerConfig) -> (NetSender, NetReceiver) {
    let secret = iroh_secret(&key);
    let id: EndpointId = secret.public().into();

    let (sender, receiver, wiring) = wire(id);

    let _thread = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async move {
            let Some(harness) = crate::transport::iroh::bind(secret, &config).await else {
                // bind already logged the failure; net thread exits.
                return;
            };
            run_host(harness, config, wiring).await;
        });
    });

    (sender, receiver)
}

// ── Network thread event loop ────────────────────────────────────────

/// Deadline for establishing + authenticating a connection (the
/// `pool_get` init future: dial + OP_AUTH round trip). A connection
/// attempt that exceeds this counts as failed: the pool's
/// singleflight cell resets so the next walk re-dials, instead of
/// every later fetch to that peer queueing forever behind one
/// stalled handshake. Generous relative to real-world QUIC + relay
/// setup times; deterministic under simulated virtual time.
const DIAL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// Deadline for a single `OP_GET_BLOB` / `OP_CHILDREN` request + full response
/// on an established connection. On expiry
/// the op reports an error and the caller's existing
/// evict-and-try-next-provider path takes over. Total-op rather than
/// progress-based: each response is bounded by the protocol's explicit
/// transport envelope; revisit with idle-deadlines when blob transfer becomes
/// chunked or streaming.
const OP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Connect to a peer over the pile-sync ALPN and immediately present
/// our capability so subsequent ops are authorised. Protocol v4 makes
/// this mandatory — the server rejects any op until the connection
/// completes auth.
#[instrument(level = "info", skip(t, self_cap), fields(peer = %hex::encode(&peer[..4])))]
async fn connect_authed<T: Transport>(
    t: &T,
    peer: PeerId,
    self_cap: &RawHash,
) -> anyhow::Result<T::Conn> {
    let conn = t.dial(peer, PILE_SYNC_ALPN).await.map_err(|e| {
        warn!(error = %e, "connect failed");
        anyhow::anyhow!("connect: {e}")
    })?;
    debug!(self_cap = %hex::encode(&self_cap[..4]), "connected; sending OP_AUTH");
    op_auth(&conn, self_cap).await.map_err(|e| {
        warn!(error = %e, "auth handshake failed");
        anyhow::anyhow!("auth: {e}")
    })?;
    info!("auth ok");
    Ok(conn)
}

async fn host_loop<T: Transport>(
    harness: Harness<T>,
    config: PeerConfig,
    commands: mpsc::Receiver<NetCommand>,
    events: mpsc::Sender<NetEvent>,
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    cap_tx: tokio::sync::watch::Sender<Option<Arc<dyn NetCapability>>>,
) {
    let Harness {
        transport,
        incoming,
        gossip,
    } = harness;

    let my_id: PeerId = transport.local_id();
    let self_cap: RawHash = config.self_cap;
    let direction = config.direction;

    // Host-wide singleflight connection pool — one authed
    // connection per remote peer, reused across all concurrent
    // legacy hint walks / swarm_fetch_chain calls. See `SharedPool`
    // docs for the OnceCell-based dial deduplication.
    let conn_pool: SharedPool<T::Conn> = new_shared_pool();

    // Gossip-suggested provider endpoints: updated by every legacy HEAD frame
    // and consulted by the on-demand fetch capability before the DHT. This is
    // bounded routing state, not provenance.
    let known_publishers: KnownPublishers = Arc::new(Mutex::new(Vec::new()));

    // Publish the inline-fetch capability now that the transport exists.
    // `Peer::fetch_blob` parks on this slot until it's filled, which is
    // how the inline read path handles the construction-ordering the old
    // `FetchBlob` command channel used to buffer past.
    let _ = cap_tx.send(Some(Arc::new(NetCap {
        transport: transport.clone(),
        pool: conn_pool.clone(),
        self_cap,
        my_id,
        publishers: known_publishers.clone(),
    }) as Arc<dyn NetCapability>));

    // Failed-walk arbitration and retry state. Every incoming observation gets
    // an attempt token; only the newest token for one exact legacy
    // `(remote id, claimed publisher)` namespace may emit a tracking event or
    // enqueue a retry. This prevents an older, slower fetch from regressing a
    // tracking pin after a newer observation has already completed.
    let retries: RetryQueue = Arc::new(Mutex::new(RetryState::default()));
    let tracking_slots: TrackingSlots = Arc::new(tokio::sync::Semaphore::new(TRACKING_WALK_LIMIT));

    // Our own pubkey — the expected `cap_subject` of any cap
    // delivered to us via OP_DELIVER_CAP.
    let our_pubkey = ed25519_dalek::VerifyingKey::from_bytes(&my_id)
        .expect("transport local id is an ed25519 pubkey");

    // ── Inbound connections: dispatch by ALPN to the protocol
    // handlers. Each connection gets its own task; each handler
    // accepts sequential bi-streams until the peer closes.
    let snapshot_handler = SnapshotHandler {
        snapshot: snapshot.clone(),
        team_root: config.team_root,
        transport: transport.clone(),
        self_cap,
        events: events.clone(),
        pool: conn_pool.clone(),
    };
    let handshake_handler = HandshakeHandler {
        events: events.clone(),
        team_root: config.team_root,
        our_pubkey,
        snapshot: snapshot.clone(),
        transport: transport.clone(),
        pool: conn_pool.clone(),
    };
    let mut incoming = incoming;
    tokio::spawn(async move {
        while let Some(inc) = incoming.recv().await {
            if inc.alpn == PILE_SYNC_ALPN {
                let h = snapshot_handler.clone();
                tokio::spawn(async move { h.handle(inc.conn).await });
            } else if inc.alpn == crate::handshake::AUTH_HANDSHAKE_ALPN {
                let h = handshake_handler.clone();
                tokio::spawn(async move { h.handle(inc.conn).await });
            } else {
                debug!(alpn = %String::from_utf8_lossy(inc.alpn), "incoming conn on unknown alpn; dropping");
            }
        }
    });

    // ── Gossip: consume the team-topic event stream. HEAD frames
    // trigger bounded content-hint walks; neighbor events are logged.
    let mut gossip_sender: Option<T::Gossip> = None;
    if let Some((sender, mut gossip_events)) = gossip {
        gossip_sender = Some(sender);
        let events_tx = events.clone();
        let t2 = transport.clone();
        // Local snapshot handle — used by the hint walker to reuse blobs we
        // already have. Same Arc the protocol server uses to answer
        // OP_GET_BLOB / OP_CHILDREN to remote peers.
        let snapshot_for_fetch = snapshot.clone();
        let pool_for_fetch = conn_pool.clone();
        let retries_for_gossip = retries.clone();
        let slots_for_gossip = tracking_slots.clone();
        let publishers_for_gossip = known_publishers.clone();
        tokio::spawn(async move {
            while let Some(event) = gossip_events.recv().await {
                match event {
                    GossipEvent::Received {
                        bytes,
                        delivered_from,
                    } => {
                        // WriteOnly still joins the mesh so it can publish, but
                        // it must not spend bandwidth fetching incoming legacy
                        // observations that the sync side will discard.
                        if direction == SyncDirection::WriteOnly {
                            continue;
                        }
                        // Gossip HEAD message, v1 (81B, 0x01) or
                        // v2 (89B, 0x02 + 8-byte nonce; the nonce is
                        // anti-dedupe padding — parsed fields are
                        // identical).
                        if (bytes.len() == 81 && bytes[0] == 0x01)
                            || (bytes.len() == 89 && bytes[0] == 0x02)
                        {
                            let mut pin = [0u8; 16];
                            pin.copy_from_slice(&bytes[1..17]);
                            let mut metadata_head = [0u8; 32];
                            metadata_head.copy_from_slice(&bytes[17..49]);
                            let mut claimed_publisher = [0u8; 32];
                            claimed_publisher.copy_from_slice(&bytes[49..81]);

                            let t3 = t2.clone();
                            let events_tx2 = events_tx.clone();
                            let self_cap2 = self_cap;
                            let snap2 = snapshot_for_fetch.clone();
                            let pool2 = pool_for_fetch.clone();
                            // Treat the in-frame publisher as a routing hint,
                            // not authenticated authorship. Try that endpoint
                            // for the legacy hint walk; if the bytes are
                            // not even a valid key, fall back to the
                            // authenticated relaying neighbor.
                            let publisher = normalize_publisher(claimed_publisher, delivered_from);
                            let fetch_peer: PeerId = publisher;
                            // Remember the publisher for the on-demand
                            // fetch path: read-miss fetches consult the
                            // gossip-known publishers before the DHT.
                            note_publisher(&publishers_for_gossip, fetch_peer);
                            // A different head supersedes queued/in-flight work
                            // for this exact publisher namespace; an identical
                            // nonce-rebroadcast coalesces with in-flight work or
                            // promotes its sleeping retry. Generation-gated
                            // completion keeps superseded tasks harmless.
                            let outcome = retries_for_gossip.lock().unwrap().begin(
                                TrackingKey { pin, publisher },
                                metadata_head,
                                fetch_peer,
                                crate::clock::mono_now(),
                            );
                            match outcome {
                                BeginOutcome::Start(attempt) => {
                                    debug!(
                                        metadata_head = %hex::encode(&metadata_head[..4]),
                                        publisher = %hex::encode(&publisher[..4]),
                                        "gossip head update; scheduling fetch"
                                    );
                                    schedule_tracking_attempt(
                                        t3,
                                        attempt,
                                        events_tx2,
                                        self_cap2,
                                        snap2,
                                        pool2,
                                        retries_for_gossip.clone(),
                                        slots_for_gossip.clone(),
                                    );
                                }
                                BeginOutcome::Coalesced => {
                                    trace!(metadata_head = %hex::encode(&metadata_head[..4]), "coalesced identical in-flight HEAD");
                                }
                                BeginOutcome::AtCapacity => {
                                    warn!(metadata_head = %hex::encode(&metadata_head[..4]), "tracking observation rejected at bounded key capacity");
                                }
                            }
                        }
                    }
                    GossipEvent::NeighborUp(peer) => {
                        info!(peer = %hex::encode(&peer[..4]), "gossip neighbor up");
                    }
                    GossipEvent::NeighborDown(peer) => {
                        info!(peer = %hex::encode(&peer[..4]), "gossip neighbor down");
                    }
                }
            }
        });
    }

    /// Build the gossip wire frame for a legacy `(pin, metadata head)` pair.
    /// v2: 0x02 | pin(16) | metadata-head(32) | publisher(32) | nonce(8) = 89 bytes.
    ///
    /// The nonce (sender's monotonic nanoseconds) makes every frame
    /// instance bytewise unique. This is load-bearing, not
    /// decoration: the gossip mesh dedupes by message id (content
    /// hash), so the periodic rebroadcast of an UNCHANGED head would
    /// otherwise be mesh-wide deduped into a no-op — and a peer that
    /// missed the original flood (crashed, partitioned, not yet
    /// joined) would never learn the head at all (bug C, 2026-06-11).
    /// With
    /// the nonce, dedupe still kills real duplicates — the same
    /// frame instance arriving via multiple mesh paths — but each
    /// rebroadcast period generates a deliverable new instance.
    fn gossip_frame(pin: &RawPinId, metadata_head: &RawHash, publisher: &PeerId) -> Vec<u8> {
        let mut msg = Vec::with_capacity(89);
        msg.push(0x02);
        msg.extend_from_slice(pin);
        msg.extend_from_slice(metadata_head);
        msg.extend_from_slice(publisher);
        msg.extend_from_slice(&crate::clock::mono_now().as_nanos().to_be_bytes());
        msg
    }

    // Last published legacy metadata HEAD per pin. Lets the periodic
    // re-broadcast tick replay our state without callers
    // having to drive it. Each replay gets a fresh nonce so it can
    // reach newly joined neighbors; mesh-level message-id deduplication
    // still collapses duplicate delivery of that particular frame.
    // BTreeMap (not HashMap): iterated on every rebroadcast tick, and
    // deterministic iteration order is required for simulation replay
    // (same seed => same frame order on the wire).
    let mut last_published_legacy_heads: std::collections::BTreeMap<RawPinId, RawHash> =
        std::collections::BTreeMap::new();
    let rebroadcast_period = std::time::Duration::from_secs(30);
    // Read through crate::clock (not std Instant) so the rebroadcast
    // tick advances under simulated virtual time.
    let mut last_rebroadcast = crate::clock::mono_now();

    // Command loop.
    loop {
        while let Ok(cmd) = commands.try_recv() {
            match cmd {
                NetCommand::Announce(hash) => {
                    let t = transport.clone();
                    tokio::spawn(async move {
                        t.dht_announce(hash).await;
                    });
                }
                NetCommand::GossipLegacyHead { pin, metadata_head } => {
                    last_published_legacy_heads.insert(pin, metadata_head);
                    if let Some(sender) = &gossip_sender {
                        let msg = gossip_frame(&pin, &metadata_head, &my_id);
                        let sender = sender.clone();
                        tokio::spawn(async move {
                            let _ = sender.broadcast(msg).await;
                        });
                    }
                }
                NetCommand::DeliverCap {
                    subject,
                    cap_bytes,
                    sig_bytes,
                } => {
                    // Open a fresh connection on the auth-handshake
                    // ALPN, send OP_DELIVER_CAP, close. On STATUS_OK
                    // ack we emit `NetEvent::CapDeliveryConfirmed`
                    // so the Peer can mark the matching
                    // renewal-policy entry as delivered; on any
                    // failure (connect/send/non-OK) the entry stays
                    // in the undelivered set and the next renewal
                    // tick attempts redispatch.
                    let t_for_deliver = transport.clone();
                    tokio::spawn(async move {
                        let conn = match t_for_deliver
                            .dial(subject, crate::handshake::AUTH_HANDSHAKE_ALPN)
                            .await
                        {
                            Ok(c) => c,
                            Err(e) => {
                                debug!(
                                    subject = %hex::encode(&subject[..4]),
                                    error = %e,
                                    "DeliverCap: connect failed"
                                );
                                return;
                            }
                        };
                        match crate::handshake::send_deliver_cap(&conn, &cap_bytes, &sig_bytes)
                            .await
                        {
                            Ok(status) if status == crate::handshake::STATUS_OK => {
                                debug!(
                                    subject = %hex::encode(&subject[..4]),
                                    "DeliverCap: recipient ack OK (wire-level — absorb \
                                     happens asynchronously on recipient; \
                                     CapDeliveryConfirmed is emitted later from the OP_AUTH \
                                     path when the subject actually authenticates with the cap)"
                                );
                            }
                            Ok(status) => {
                                debug!(
                                    subject = %hex::encode(&subject[..4]),
                                    status,
                                    "DeliverCap: recipient returned non-OK status"
                                );
                            }
                            Err(e) => {
                                debug!(
                                    subject = %hex::encode(&subject[..4]),
                                    error = %e,
                                    "DeliverCap: send failed"
                                );
                            }
                        }
                        conn.close(0, b"ok");
                    });
                }
            }
        }

        // ── Failed-walk retries: respawn walks whose backoff expired.
        {
            let now = crate::clock::mono_now();
            loop {
                let Ok(permit) = tracking_slots.clone().try_acquire_owned() else {
                    break;
                };
                let Some(attempt) = retries.lock().unwrap().take_one_due(now) else {
                    drop(permit);
                    break;
                };
                debug!(
                    metadata_head = %hex::encode(&attempt.metadata_head[..4]),
                    attempt = attempt.attempt,
                    "retrying failed walk"
                );
                let t3 = transport.clone();
                let events_tx2 = events.clone();
                let self_cap2 = self_cap;
                let snap2 = snapshot.clone();
                let pool2 = conn_pool.clone();
                let retries2 = retries.clone();
                spawn_tracking_attempt(
                    t3, attempt, events_tx2, self_cap2, snap2, pool2, retries2, permit,
                );
            }
        }

        if crate::clock::mono_now().duration_since(last_rebroadcast) >= rebroadcast_period {
            trace!(
                n = last_published_legacy_heads.len(),
                "rebroadcast tick: replaying published heads"
            );
            if let Some(sender) = &gossip_sender {
                for (pin, metadata_head) in &last_published_legacy_heads {
                    let msg = gossip_frame(pin, metadata_head, &my_id);
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let _ = sender.broadcast(msg).await;
                    });
                }
            }
            last_rebroadcast = crate::clock::mono_now();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

struct FetchedSubgraph {
    blobs: std::collections::BTreeMap<RawHash, anybytes::Bytes>,
}

#[derive(Clone, Copy)]
struct HintWalkLimits {
    max_blobs: usize,
    max_bytes: usize,
}

const HINT_WALK_LIMITS: HintWalkLimits = HintWalkLimits {
    max_blobs: 65_536,
    max_bytes: MAX_GET_BLOB_BYTES,
};

/// One legacy hint walk is provisional transport work, not an unbounded
/// replication promise. Its wall-clock bound also guarantees an execution
/// permit is eventually released even if a content-valid hostile graph keeps
/// yielding fresh hints. A very large already-local prefix can still consume
/// the deadline on every retry; a persisted traversal cursor is the eventual
/// fix for that re-walk cost, while streamed blobs make network progress
/// monotone today.
const HINT_WALK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2 * 60);

struct HintWalkBudget {
    limits: HintWalkLimits,
    blobs: usize,
    bytes: usize,
}

impl HintWalkBudget {
    fn new(limits: HintWalkLimits) -> Self {
        Self {
            limits,
            blobs: 0,
            bytes: 0,
        }
    }

    /// Admit another network fetch. Local blobs never consume this budget:
    /// they are durable progress from an earlier slice, so charging them again
    /// would make a hinted subgraph larger than one slice permanently
    /// impossible.
    fn admit_fetch(&self) -> anyhow::Result<()> {
        if self.blobs >= self.limits.max_blobs || self.bytes >= self.limits.max_bytes {
            return Err(anyhow::anyhow!(
                "hint walk budget exhausted (fetched blobs {}/{}, bytes {}/{})",
                self.blobs,
                self.limits.max_blobs,
                self.bytes,
                self.limits.max_bytes,
            ));
        }
        Ok(())
    }

    /// Record one completed, content-verified network fetch. The static wire
    /// limit bounds a single-blob overshoot of the byte slice; the blob is
    /// retained as useful progress and the next fetch observes exhaustion.
    fn record_fetch(&mut self, bytes: usize) -> anyhow::Result<()> {
        self.blobs = self
            .blobs
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("hint walk blob count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("hint walk byte count overflow"))?;
        Ok(())
    }
}

/// Every aligned 32-byte word is a conservative potential blob reference.
///
/// A remote child hint is accepted only if it occurs in this intrinsic
/// candidate set; whether the candidate is an actual child remains
/// store-relative, matching `BlobChildren`.
fn potential_child_hashes(bytes: &[u8]) -> impl Iterator<Item = RawHash> + '_ {
    bytes.chunks_exact(32).filter_map(|chunk| {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(chunk);
        (hash != NIL_HASH).then_some(hash)
    })
}

/// Follow one bounded slice of content-bound child hints.
///
/// `OP_CHILDREN` is only a batching hint: each hinted hash must occur in the
/// already content-verified parent and its own bytes must hash correctly. An
/// empty or failed publisher hint falls through to DHT providers. This cannot
/// prove that an untrusted provider enumerated every store-relative child—the
/// `UnknownBlob` model has no intrinsic typed edge set—but it avoids both
/// invented edges and per-word network probes over arbitrary binary blobs. A
/// successful walk means that every currently fetchable, content-bound hint in
/// this slice was processed; it is deliberately not a proof of global closure.
async fn fetch_hinted_subgraph<T, L, S>(
    t: &T,
    publisher: PeerId,
    root: &RawHash,
    pool: &SharedPool<T::Conn>,
    self_cap: &RawHash,
    local_blob: L,
    mut on_fetched: S,
    limits: HintWalkLimits,
) -> anyhow::Result<FetchedSubgraph>
where
    T: Transport,
    L: Fn(&RawHash) -> Option<Vec<u8>>,
    S: FnMut(RawHash, anybytes::Bytes),
{
    let mut budget = HintWalkBudget::new(limits);
    let (root_bytes, root_fetched) = match local_blob(root) {
        Some(bytes) => (anybytes::Bytes::from_source(bytes), false),
        None => {
            budget.admit_fetch()?;
            let Some(bytes) = fetch_one(t, root, pool, publisher, self_cap).await else {
                return Err(anyhow::anyhow!(
                    "root blob unavailable from all known providers: {}",
                    hex::encode(root)
                ));
            };
            (anybytes::Bytes::from_source(bytes), true)
        }
    };

    if root_fetched {
        budget.record_fetch(root_bytes.len())?;
        on_fetched(*root, root_bytes.clone());
    }
    let mut seen = HashSet::from([*root]);
    let mut traversal = vec![*root];
    let mut blobs = std::collections::BTreeMap::from([(*root, root_bytes)]);
    let mut cursor = 0usize;

    while cursor < traversal.len() {
        let parent = traversal[cursor];
        cursor += 1;
        let parent_bytes = blobs
            .get(&parent)
            .expect("every discovered hash has verified bytes");
        let accepted = children_one(t, &parent, parent_bytes, pool, publisher, self_cap)
            .await
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no provider answered child-hint request for {}",
                    hex::encode(parent)
                )
            })?;

        for child in accepted {
            if !seen.insert(child) {
                continue;
            }
            let (bytes, fetched) = match local_blob(&child) {
                Some(bytes) => (anybytes::Bytes::from_source(bytes), false),
                None => {
                    budget.admit_fetch()?;
                    let Some(bytes) = fetch_one(t, &child, pool, publisher, self_cap).await else {
                        // A hint is not an authoritative edge: it proves only
                        // that an untrusted responder named a hash occurring in
                        // the verified parent bytes. Treating a later miss as a
                        // hard walk failure would let any authorised peer
                        // force retry-DoS with an incidental aligned word.
                        // Periodic gossip can discover a transiently missing
                        // real child on a later bounded walk.
                        continue;
                    };
                    (anybytes::Bytes::from_source(bytes), true)
                }
            };
            if fetched {
                budget.record_fetch(bytes.len())?;
                on_fetched(child, bytes.clone());
            }
            traversal.push(child);
            blobs.insert(child, bytes);
        }
    }

    Ok(FetchedSubgraph { blobs })
}

/// Fetch one bounded, swarm-distributed hint walk from a legacy HEAD.
///
/// For each blob along the BFS, tries the frame's publisher hint first and, on
/// failure, asks the DHT for distinct fallback providers. A host-wide connection
/// pool keyed on `EndpointId` ensures we only auth once per
/// provider — subsequent ops to the same provider reuse the
/// connection through iroh's QUIC stream multiplexing (our
/// `SnapshotHandler` already accepts unbounded sequential
/// bi-streams per connection; auth state is per-connection, set
/// on the first OP_AUTH stream).
///
/// Earlier versions opened one fresh `connect_authed` per blob,
/// paying ~600ms of auth handshake each. A BFS over even a small
/// graph would exhaust an outer deadline before the walk
/// completed. With the pool, one auth per provider covers any
/// number of ops. DHT-driven fallback also lets the walk recover
/// through caching peers instead of depending on the publisher
/// remaining online.
async fn walk_legacy_hints<T: Transport>(
    t: &T,
    publisher: PeerId,
    metadata_head: &RawHash,
    events: &mpsc::Sender<NetEvent>,
    self_cap: &RawHash,
    local: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: &SharedPool<T::Conn>,
) -> anyhow::Result<()> {
    // Local lookup against the same snapshot the server uses to answer remote
    // reads. A local parent does not short-circuit discovery: `UnknownBlob`
    // children are store-relative, so local presence alone cannot prove a
    // globally complete reachable graph.
    let local_blob = |hash: &RawHash| -> Option<Vec<u8>> {
        local
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|s| s.get_blob(hash))
    };

    tokio::time::timeout(
        HINT_WALK_DEADLINE,
        fetch_hinted_subgraph(
            t,
            publisher,
            metadata_head,
            pool,
            self_cap,
            local_blob,
            |_, bytes| {
                // Content-addressed blobs are monotone partial progress. They
                // may safely land even when this slice later exhausts its
                // budget, times out, or is superseded. Only Head is gated on a
                // completed, still-current bounded hint walk.
                let _ = events.send(NetEvent::Blob(bytes));
            },
            HINT_WALK_LIMITS,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("hint walk exceeded {HINT_WALK_DEADLINE:?}"))??;

    // No close: connections live in the shared pool for the
    // host_loop's lifetime, reused by subsequent walks.
    Ok(())
}

/// Resolve distinct DHT fallbacks after a publisher attempt failed.
///
/// The lookup is deliberately lazy: the ordinary live-publisher path pays no
/// DHT latency. A stale, offline, or spoofed publisher hint still falls through
/// to healthy caches elsewhere in the swarm, and a dark DHT is deadline-bounded.
///
/// Self is filtered out — `find_providers` will list us as a
/// provider for any blob we've announced, and trying to dial
/// ourselves trips iroh's "Connecting to ourself is not supported"
/// error. Local lookup is handled before network resolution where available,
/// so self is never a useful fallback endpoint.
async fn dht_fallback_providers<T: Transport>(
    t: &T,
    hash: &RawHash,
    publisher_id: PeerId,
) -> Vec<PeerId> {
    let my_id = t.local_id();
    trace!(hash = %hex::encode(&hash[..4]), "DHT fallback lookup awaiting");
    let discovered: Vec<PeerId> =
        match tokio::time::timeout(std::time::Duration::from_secs(3), t.dht_providers(*hash)).await
        {
            Ok(p) => p,
            Err(_) => {
                warn!(
                    hash = %hex::encode(&hash[..4]),
                    "dht_providers timed out; no provider candidates"
                );
                Vec::new()
            }
        };
    trace!(hash = %hex::encode(&hash[..4]), n = discovered.len(), "DHT fallback lookup returned");
    dht_fallback_candidates(my_id, publisher_id, discovered)
}

fn dht_fallback_candidates(
    my_id: PeerId,
    publisher_id: PeerId,
    discovered: impl IntoIterator<Item = PeerId>,
) -> Vec<PeerId> {
    let mut providers = Vec::new();
    let mut seen = HashSet::new();
    for provider in discovered {
        if provider != my_id && provider != publisher_id && seen.insert(provider) {
            providers.push(provider);
            if providers.len() == PROVIDER_FANOUT_CAP {
                break;
            }
        }
    }
    providers
}

/// Host-wide connection pool: one authed `iroh::endpoint::Connection`
/// per remote peer, shared across all concurrent legacy hint walks /
/// `swarm_fetch_chain` invocations.
///
/// `OnceCell` per peer provides automatic singleflight: the first
/// task to encounter a missing entry runs the dial; concurrent tasks
/// await the same `OnceCell` and reuse the resulting connection. No
/// dial-storm when a gossip rebroadcast fans 5+ heads into 5+ parallel
/// fetch tasks targeting the same peer.
///
/// iroh QUIC multiplexes streams cheaply on a single connection; our
/// `serve_stream` accepts unbounded sequential bi-streams per
/// connection (auth state set on the first OP_AUTH stream, reused on
/// every subsequent stream). So one connection per peer is enough.
pub(crate) type SharedPool<C> =
    Arc<tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::OnceCell<C>>>>>;

fn new_shared_pool<C>() -> SharedPool<C> {
    Arc::new(tokio::sync::Mutex::new(HashMap::new()))
}

/// Get-or-dial an authed connection to `provider` from the shared
/// pool. `OnceCell::get_or_try_init` runs the dial exactly once even
/// if many tasks race here concurrently; the rest await the same
/// initialization. Returns `None` if the dial fails (the cell stays
/// uninitialized so a later call can retry).
async fn pool_get<T: Transport>(
    t: &T,
    pool: &SharedPool<T::Conn>,
    provider: PeerId,
    self_cap: &RawHash,
) -> Option<T::Conn> {
    let cell = {
        let mut guard = pool.lock().await;
        guard
            .entry(provider)
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone()
    };
    let init = || async {
        match tokio::time::timeout(DIAL_DEADLINE, connect_authed(t, provider, self_cap)).await {
            Ok(r) => r,
            Err(_) => Err(anyhow::anyhow!(
                "connection setup deadline ({DIAL_DEADLINE:?}) exceeded"
            )),
        }
    };
    match cell.get_or_try_init(init).await {
        Ok(conn) => Some(conn.clone()),
        Err(e) => {
            debug!(error = %e, provider = %hex::encode(&provider[..4]), "pool dial failed");
            // Drop the cell so the next caller can retry. Use a fresh
            // entry: if anyone awaited the original cell while we were
            // in get_or_try_init, they all got the same Err — they'll
            // retry through their own entries below.
            let mut guard = pool.lock().await;
            if let Some(existing) = guard.get(&provider) {
                if std::ptr::eq(Arc::as_ptr(existing), Arc::as_ptr(&cell)) {
                    guard.remove(&provider);
                }
            }
            None
        }
    }
}

/// Evict a connection from the pool. Called when an op on the pooled
/// connection errors (peer may have closed, network changed, etc.)
/// so the next access re-dials.
async fn pool_evict<C: Conn>(pool: &SharedPool<C>, provider: PeerId) {
    let removed = {
        let mut guard = pool.lock().await;
        guard.remove(&provider)
    };
    if let Some(cell) = removed {
        if let Some(conn) = cell.get() {
            conn.close(0, b"pool evict");
        }
    }
}

/// Fetch a single blob via the swarm: publisher hint first, then distinct DHT
/// fallbacks. Returns the first content-verified response.
async fn fetch_one<T: Transport>(
    t: &T,
    hash: &RawHash,
    pool: &SharedPool<T::Conn>,
    publisher_id: PeerId,
    self_cap: &RawHash,
) -> Option<Vec<u8>> {
    if publisher_id != t.local_id()
        && let Some(data) = fetch_from_providers(t, hash, pool, &[publisher_id], self_cap).await
    {
        return Some(data);
    }
    let fallbacks = dht_fallback_providers(t, hash, publisher_id).await;
    fetch_from_providers(t, hash, pool, &fallbacks, self_cap).await
}

/// Try `providers` in order for a single blob: pooled authed connection,
/// OP_GET_BLOB with the per-op deadline, evict-and-try-next on
/// connection errors or hash mismatches. First content-verified success wins.
/// The provider-iteration tail of [`fetch_one`], split out so the
/// publisher-first on-demand path ([`NetCap::fetch_blob`]) can drive it
/// with gossip-known candidates without a DHT round-trip.
async fn fetch_from_providers<T: Transport>(
    t: &T,
    hash: &RawHash,
    pool: &SharedPool<T::Conn>,
    providers: &[PeerId],
    self_cap: &RawHash,
) -> Option<Vec<u8>> {
    for &provider in providers {
        let Some(conn) = pool_get(t, pool, provider, self_cap).await else {
            continue;
        };
        let op = tokio::time::timeout(OP_DEADLINE, op_get_blob(&conn, hash))
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "OP_GET_BLOB deadline ({OP_DEADLINE:?}) exceeded"
                ))
            });
        match op {
            Ok(Some(data)) if blake3::hash(&data).as_bytes() == hash => return Some(data),
            Ok(Some(_)) => {
                warn!(
                    hash = %hex::encode(&hash[..4]),
                    provider = %hex::encode(&provider[..4]),
                    "provider returned bytes with the wrong content hash; evicting"
                );
                pool_evict(pool, provider).await;
                continue;
            }
            Ok(None) => {
                debug!(hash = %hex::encode(&hash[..4]), provider = %hex::encode(&provider[..4]), "blob miss");
                continue;
            }
            Err(e) => {
                debug!(error = %e, hash = %hex::encode(&hash[..4]), provider = %hex::encode(&provider[..4]), "op_get_blob errored, evicting and trying next provider");
                // Connection-level error: pooled connection may be
                // dead. Evict so subsequent ops to this peer re-dial.
                pool_evict(pool, provider).await;
                continue;
            }
        }
    }
    None
}

/// Swarm-fetch a hinted subgraph rooted at `head` (a cap sig handle, in the
/// OP_AUTH context) and return it as a `BTreeMap<RawHash, Bytes>`.
/// Uses the same content-bound discovery as the legacy hint walker, but writes
/// the result to a map instead of emitting `NetEvent::Blob`. The caller verifies
/// whether the returned subgraph contains the required capability chain and
/// decides whether to cache the bytes after using them.
async fn swarm_fetch_chain<T: Transport>(
    t: &T,
    publisher: PeerId,
    head: &RawHash,
    self_cap: &RawHash,
    pool: &SharedPool<T::Conn>,
) -> std::collections::BTreeMap<RawHash, anybytes::Bytes> {
    match tokio::time::timeout(
        HINT_WALK_DEADLINE,
        fetch_hinted_subgraph(
            t,
            publisher,
            head,
            pool,
            self_cap,
            |_| None,
            |_, _| {},
            HINT_WALK_LIMITS,
        ),
    )
    .await
    {
        Ok(Ok(subgraph)) => subgraph.blobs,
        Ok(Err(error)) => {
            debug!(%error, head = %hex::encode(&head[..4]), "cap hinted subgraph unavailable");
            std::collections::BTreeMap::new()
        }
        Err(_) => {
            warn!(head = %hex::encode(&head[..4]), "cap hint walk exceeded deadline");
            std::collections::BTreeMap::new()
        }
    }
}

/// Ask for store-relative child hints. The claimed publisher is the low-latency
/// first choice; an empty or failed response cannot suppress DHT fallbacks.
/// Non-empty hints are still filtered against content-verified parent bytes by
/// [`fetch_hinted_subgraph`] before they influence traversal.
async fn children_one<T: Transport>(
    t: &T,
    parent: &RawHash,
    parent_bytes: &[u8],
    pool: &SharedPool<T::Conn>,
    publisher_id: PeerId,
    self_cap: &RawHash,
) -> Option<Vec<RawHash>> {
    let publisher_hint = if publisher_id != t.local_id() {
        child_hints_from_providers(t, parent, pool, &[publisher_id], self_cap)
            .await
            .map(|hints| content_bound_hints(parent, parent_bytes, hints))
    } else {
        None
    };
    if publisher_hint
        .as_ref()
        .is_some_and(|hints| !hints.is_empty())
    {
        return publisher_hint;
    }

    trace!(parent = %hex::encode(&parent[..4]), "child hints: DHT fallbacks awaiting");
    let providers = dht_fallback_providers(t, parent, publisher_id).await;
    trace!(parent = %hex::encode(&parent[..4]), n = providers.len(), "child hints: DHT fallbacks returned");
    let fallback = child_hints_from_providers(t, parent, pool, &providers, self_cap)
        .await
        .map(|hints| content_bound_hints(parent, parent_bytes, hints));
    fallback.or(publisher_hint)
}

/// Intersect a wire-bounded hint set with aligned words in verified parent
/// bytes. The inversion is deliberate: materialising every candidate word from
/// a 256 MiB arbitrary blob would itself consume hundreds of MiB, while the
/// untrusted side is capped at [`MAX_CHILD_HINTS`].
fn content_bound_hints(parent: &RawHash, parent_bytes: &[u8], hints: Vec<RawHash>) -> Vec<RawHash> {
    if hints.is_empty() {
        return hints;
    }
    let hinted_count = hints.len();
    let hints: HashSet<_> = hints.into_iter().collect();
    let mut accepted = std::collections::BTreeSet::new();
    for candidate in potential_child_hashes(parent_bytes) {
        if hints.contains(&candidate) {
            accepted.insert(candidate);
            if accepted.len() == hints.len() {
                break;
            }
        }
    }
    let discarded = hinted_count.saturating_sub(accepted.len());
    if discarded != 0 {
        warn!(
            parent = %hex::encode(&parent[..4]),
            discarded,
            "ignoring child hints absent from content-verified parent"
        );
    }
    accepted.into_iter().collect()
}

/// Union successful child-hint responses from all supplied providers.
///
/// An empty response is not authoritative, so it never prevents consulting a
/// later provider. Returning `Some(empty)` only records that at least one peer
/// answered; callers give it the same leaf-like behavior as no hint.
async fn child_hints_from_providers<T: Transport>(
    t: &T,
    parent: &RawHash,
    pool: &SharedPool<T::Conn>,
    providers: &[PeerId],
    self_cap: &RawHash,
) -> Option<Vec<RawHash>> {
    let mut answered = false;
    let mut hints = std::collections::BTreeSet::new();
    for &provider in providers {
        let Some(conn) = pool_get(t, pool, provider, self_cap).await else {
            continue;
        };
        let op = tokio::time::timeout(OP_DEADLINE, op_children(&conn, parent))
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "OP_CHILDREN deadline ({OP_DEADLINE:?}) exceeded"
                ))
            });
        match op {
            Ok(children) => {
                answered = true;
                for child in children {
                    if hints.len() == MAX_CHILD_HINTS {
                        break;
                    }
                    hints.insert(child);
                }
            }
            Err(error) => {
                debug!(%error, parent = %hex::encode(&parent[..4]), provider = %hex::encode(&provider[..4]), "OP_CHILDREN errored; evicting provider");
                pool_evict(pool, provider).await;
            }
        }
    }
    answered.then(|| hints.into_iter().collect())
}

/// Exact namespace of one legacy tracking observation.
///
/// The 16-byte id belongs to the claimed publisher's namespace. Treating the id
/// alone as a retry key lets two publishers cancel each other's work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TrackingKey {
    pin: RawPinId,
    publisher: PublisherKey,
}

/// One generation-tagged legacy hint-walk attempt.
///
/// Tokens are opaque evidence that the attempt was current when it began. A
/// later observation for the same [`TrackingKey`] replaces the active
/// generation; both success and failure of the old token then become no-ops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrackingAttempt {
    key: TrackingKey,
    metadata_head: RawHash,
    fetch_peer: PeerId,
    generation: u64,
    attempt: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActiveObservation {
    metadata_head: RawHash,
    generation: u64,
    /// Fixed admission lease. Replays and head churn on an already admitted
    /// key deliberately do not extend it.
    expires_at: crate::clock::Mono,
}

#[derive(Clone, Copy, Debug)]
struct RetryEntry {
    attempt: TrackingAttempt,
    next_attempt: crate::clock::Mono,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BeginOutcome {
    Start(TrackingAttempt),
    Coalesced,
    AtCapacity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureOutcome {
    Queued,
    Stale,
    Exhausted,
}

/// At most this many unauthenticated legacy namespaces occupy retry state.
const TRACKING_KEY_CAP: usize = 1024;
/// At most this many hint walks execute concurrently. Admission happens
/// before spawning, so there is no unbounded queue of parked Tokio tasks.
const TRACKING_WALK_LIMIT: usize = 16;
/// A failed observation lives for a fixed horizon, long enough to span several
/// 30-second gossip rebroadcasts and capped-backoff attempts. Replays do not
/// renew the lease; after expiry a later frame may be admitted afresh.
const TRACKING_LEASE: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Bound retries within one lease even if the clock advances unusually slowly.
const MAX_TRACKING_ATTEMPTS: u32 = 8;

/// Current in-flight observations plus their queued retry, if any.
///
/// Successful observations are removed, so unauthenticated gossip cannot grow
/// a permanent seen-key ledger. `next_generation` stays scalar while ensuring
/// a very old token cannot alias a later observation after its key was removed
/// and reinserted.
struct RetryState {
    next_generation: u64,
    active: std::collections::BTreeMap<TrackingKey, ActiveObservation>,
    pending: std::collections::BTreeMap<TrackingKey, RetryEntry>,
    key_cap: usize,
    lease: std::time::Duration,
}

impl Default for RetryState {
    fn default() -> Self {
        Self {
            next_generation: 0,
            active: std::collections::BTreeMap::new(),
            pending: std::collections::BTreeMap::new(),
            key_cap: TRACKING_KEY_CAP,
            lease: TRACKING_LEASE,
        }
    }
}

impl RetryState {
    #[cfg(test)]
    fn with_limits(key_cap: usize, lease: std::time::Duration) -> Self {
        Self {
            key_cap,
            lease,
            ..Self::default()
        }
    }

    fn next_generation(&mut self) -> u64 {
        self.next_generation = self.next_generation.wrapping_add(1);
        // Generation zero is reserved only to make accidental default-like
        // tokens conspicuous in debugging. Wrapping here would require 2^64
        // observations in one process lifetime.
        if self.next_generation == 0 {
            self.next_generation = 1;
        }
        self.next_generation
    }

    fn prune_expired(&mut self, now: crate::clock::Mono) {
        let expired: Vec<_> = self
            .active
            .iter()
            .filter(|(_, observation)| observation.expires_at <= now)
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            self.active.remove(&key);
            self.pending.remove(&key);
        }
    }

    fn begin(
        &mut self,
        key: TrackingKey,
        metadata_head: RawHash,
        fetch_peer: PeerId,
        now: crate::clock::Mono,
    ) -> BeginOutcome {
        self.prune_expired(now);

        if let Some(active) = self.active.get(&key).copied() {
            if active.metadata_head == metadata_head {
                // An exact rebroadcast while a retry is sleeping is a useful
                // liveness signal: promote that already-counted retry now.
                if let Some(mut entry) = self.pending.remove(&key) {
                    entry.attempt.fetch_peer = fetch_peer;
                    return BeginOutcome::Start(entry.attempt);
                }
                // No pending entry means the exact generation is already in
                // flight. The nonce made delivery fresh, not the work.
                return BeginOutcome::Coalesced;
            }
        } else if self.active.len() >= self.key_cap {
            return BeginOutcome::AtCapacity;
        }

        let generation = self.next_generation();
        let expires_at = self
            .active
            .get(&key)
            .map(|observation| observation.expires_at)
            .unwrap_or(now + self.lease);
        self.active.insert(
            key,
            ActiveObservation {
                metadata_head,
                generation,
                expires_at,
            },
        );
        self.pending.remove(&key);
        BeginOutcome::Start(TrackingAttempt {
            key,
            metadata_head,
            fetch_peer,
            generation,
            attempt: 0,
        })
    }

    fn is_current(&self, attempt: &TrackingAttempt) -> bool {
        self.active.get(&attempt.key).is_some_and(|active| {
            active.metadata_head == attempt.metadata_head && active.generation == attempt.generation
        })
    }

    /// Finish a successful attempt. The caller must retain the surrounding
    /// mutex guard while emitting the resulting `NetEvent::LegacyHead`; otherwise a
    /// new observation could become current between this check and the send.
    fn complete_success(&mut self, attempt: &TrackingAttempt, now: crate::clock::Mono) -> bool {
        self.prune_expired(now);
        if !self.is_current(attempt) {
            return false;
        }
        self.active.remove(&attempt.key);
        self.pending.remove(&attempt.key);
        true
    }

    fn complete_failure(
        &mut self,
        attempt: TrackingAttempt,
        now: crate::clock::Mono,
    ) -> FailureOutcome {
        self.prune_expired(now);
        if !self.is_current(&attempt) {
            return FailureOutcome::Stale;
        }
        if attempt.attempt.saturating_add(1) >= MAX_TRACKING_ATTEMPTS {
            self.active.remove(&attempt.key);
            self.pending.remove(&attempt.key);
            return FailureOutcome::Exhausted;
        }
        let next_attempt = TrackingAttempt {
            attempt: attempt.attempt.saturating_add(1),
            ..attempt
        };
        self.pending.insert(
            attempt.key,
            RetryEntry {
                attempt: next_attempt,
                next_attempt: now + retry_backoff(attempt.attempt),
            },
        );
        FailureOutcome::Queued
    }

    /// Put an admitted attempt back at the front of the deterministic retry
    /// queue because no execution permit was available. This is scheduling,
    /// not a network failure, so it neither increments attempt count nor
    /// changes backoff history.
    fn defer(&mut self, attempt: TrackingAttempt, now: crate::clock::Mono) -> bool {
        self.prune_expired(now);
        if !self.is_current(&attempt) {
            return false;
        }
        self.pending.insert(
            attempt.key,
            RetryEntry {
                attempt,
                next_attempt: now,
            },
        );
        true
    }

    fn take_one_due(&mut self, now: crate::clock::Mono) -> Option<TrackingAttempt> {
        self.prune_expired(now);
        let key = self
            .pending
            .iter()
            .filter(|(_, entry)| entry.next_attempt <= now)
            .map(|(key, _)| *key)
            .next()?;
        let entry = self.pending.remove(&key)?;
        self.is_current(&entry.attempt).then_some(entry.attempt)
    }
}

type RetryQueue = Arc<Mutex<RetryState>>;
type TrackingSlots = Arc<tokio::sync::Semaphore>;

#[allow(clippy::too_many_arguments)]
fn spawn_tracking_attempt<T: Transport>(
    t: T,
    attempt: TrackingAttempt,
    events: mpsc::Sender<NetEvent>,
    self_cap: RawHash,
    local: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: SharedPool<T::Conn>,
    retries: RetryQueue,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    tokio::spawn(async move {
        // Admission is acquired before spawning; retaining the owned permit for
        // the whole walk bounds actual work rather than merely bounding pollers.
        let _permit = permit;
        track_legacy_head(&t, &attempt, &events, &self_cap, &local, &pool, &retries).await;
    });
}

#[allow(clippy::too_many_arguments)]
fn schedule_tracking_attempt<T: Transport>(
    t: T,
    attempt: TrackingAttempt,
    events: mpsc::Sender<NetEvent>,
    self_cap: RawHash,
    local: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: SharedPool<T::Conn>,
    retries: RetryQueue,
    slots: TrackingSlots,
) {
    match slots.try_acquire_owned() {
        Ok(permit) => {
            spawn_tracking_attempt(t, attempt, events, self_cap, local, pool, retries, permit)
        }
        Err(_) => {
            if retries
                .lock()
                .unwrap()
                .defer(attempt, crate::clock::mono_now())
            {
                trace!(metadata_head = %hex::encode(&attempt.metadata_head[..4]), "tracking walk deferred at concurrency limit");
            }
        }
    }
}

fn retry_backoff(attempt: u32) -> std::time::Duration {
    crate::RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(6))
        .min(crate::RETRY_BACKOFF_CAP)
}

#[allow(clippy::too_many_arguments)]
async fn track_legacy_head<T: Transport>(
    t: &T,
    attempt: &TrackingAttempt,
    events: &mpsc::Sender<NetEvent>,
    self_cap: &RawHash,
    local: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: &SharedPool<T::Conn>,
    retries: &RetryQueue,
) {
    if let Err(e) = walk_legacy_hints(
        t,
        attempt.fetch_peer,
        &attempt.metadata_head,
        events,
        self_cap,
        local,
        pool,
    )
    .await
    {
        warn!(
            error = %e,
            peer = %hex::encode(&attempt.fetch_peer[..4]),
            attempt = attempt.attempt,
            "legacy hint walk failed; applying bounded retry policy"
        );
        match retries
            .lock()
            .unwrap()
            .complete_failure(*attempt, crate::clock::mono_now())
        {
            FailureOutcome::Queued => {}
            FailureOutcome::Stale => {
                debug!(
                    metadata_head = %hex::encode(&attempt.metadata_head[..4]),
                    "discarding failure from superseded legacy HEAD fetch"
                );
            }
            FailureOutcome::Exhausted => {
                warn!(
                    metadata_head = %hex::encode(&attempt.metadata_head[..4]),
                    "legacy HEAD fetch exhausted its bounded retry lease"
                );
            }
        }
    } else {
        // Keep the arbitration guard through the non-blocking std-mpsc send.
        // A fresh observation cannot become current between the generation
        // check and event enqueue, so channel order agrees with observation
        // order even when hint walks complete out of order.
        let mut state = retries.lock().unwrap();
        if state.complete_success(attempt, crate::clock::mono_now()) {
            let _ = events.send(NetEvent::LegacyHead {
                pin: attempt.key.pin,
                metadata_head: attempt.metadata_head,
                publisher: attempt.key.publisher,
            });
        } else {
            debug!(
                metadata_head = %hex::encode(&attempt.metadata_head[..4]),
                "discarding success from superseded legacy HEAD fetch"
            );
        }
    }
}

// ── Protocol handler ─────────────────────────────────────────────────

#[derive(Clone)]
struct SnapshotHandler<T: Transport> {
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Verifies all incoming capability chains. Required — protocol v4
    /// has mandatory auth.
    team_root: ed25519_dalek::VerifyingKey,
    /// Transport for outbound connections + DHT provider lookup
    /// during the swarm-fetch fallback in OP_AUTH (when an incoming
    /// cap chain references blobs we don't have locally).
    transport: T,
    /// Our own cap handle, presented at OP_AUTH when we dial peers
    /// to fetch missing cap chain blobs.
    self_cap: RawHash,
    /// Channel back to the Peer for caching fetched cap blobs. After
    /// a successful swarm-fetch + verify_chain, we emit NetEvent::Blob
    /// for each fetched cap so the Peer puts them in the local store —
    /// next OP_AUTH involving the same chain hits local instead of
    /// re-walking the swarm.
    events: mpsc::Sender<NetEvent>,
    /// Host-wide connection pool. Shared with the gossip-arrival
    /// fetch path. The OP_AUTH swarm-fetch and the gossip-driven
    /// fetch end up using the same authed connection per peer.
    pool: SharedPool<T::Conn>,
}

/// Protocol handler for `/triblespace/auth-handshake/1`. Accepts
/// incoming `OP_REQUEST_CAP` and `OP_DELIVER_CAP` streams and
/// forwards their payloads to the Peer's event channel. All policy
/// (approve / queue / reject; verify / pin / drop) lives in the
/// receiving Peer, not here — this handler just bridges the wire to
/// the local event queue.
#[derive(Clone)]
struct HandshakeHandler<T: Transport> {
    events: mpsc::Sender<NetEvent>,
    /// Team root pubkey — verifies the delivered cap's chain at
    /// `OP_DELIVER_CAP` time so STATUS_OK means "we'd accept this".
    team_root: ed25519_dalek::VerifyingKey,
    /// Our own pubkey — the expected `cap_subject` of any cap
    /// delivered to us.
    our_pubkey: ed25519_dalek::VerifyingKey,
    /// Snapshot for local-pile blob lookup during verify.
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Transport + pool are the swarm-fetch substrate. When the
    /// local-pile verify fails with `Fetch`, we first ask the dialer,
    /// then fall back to distinct DHT providers of the missing blobs,
    /// and derive the chain from content-bound `OP_CHILDREN` hints plus
    /// content-verified `OP_GET_BLOB` responses until we have everything
    /// verify needs. The
    /// swarm-fetch credential is the just-delivered sig handle
    /// itself (see the OP_DELIVER_CAP arm), so no self_cap here.
    transport: T,
    pool: SharedPool<T::Conn>,
}

impl<T: Transport> HandshakeHandler<T> {
    async fn handle(&self, connection: T::Conn) {
        // PublisherKey is just the 32-byte pubkey representation;
        // the transport's remote id is the TLS-verified ed25519
        // pubkey of the dialer (matched against the type alias in
        // channel.rs).
        let peer_pubkey_bytes: PublisherKey = connection.remote_id();
        let events = self.events.clone();
        let team_root = self.team_root;
        let our_pubkey = self.our_pubkey;
        let snapshot = self.snapshot.clone();
        let transport = self.transport.clone();
        let pool = self.pool.clone();
        let span = info_span!(
            "auth-handshake",
            peer = %hex::encode(&peer_pubkey_bytes[..4]),
        );
        async move {
            // Each connection can carry multiple bi-streams (e.g. a
            // request followed by a deliver). Loop until the peer
            // closes the connection.
            loop {
                let Some((mut send, mut recv)) = connection.accept_bi().await else {
                    debug!("accept_bi ended; handshake connection closing");
                    break;
                };
                match crate::handshake::read_incoming(&mut recv).await {
                    Ok(Some(crate::handshake::IncomingOp::Request {
                        partial_cap_bytes,
                    })) => {
                        let _ = events.send(NetEvent::CapRequest {
                            requester: peer_pubkey_bytes,
                            partial_cap_bytes,
                        });
                        let _ = crate::handshake::respond(
                            &mut send,
                            crate::handshake::STATUS_OK,
                        )
                        .await;
                    }
                    Ok(Some(crate::handshake::IncomingOp::Deliver {
                        cap_bytes,
                        sig_bytes,
                    })) => {
                        use triblespace_core::blob::{Blob, TryFromBlob};
                        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
                        use triblespace_core::inline::Inline;
                        use triblespace_core::inline::encodings::hash::Handle;
                        use triblespace_core::trible::TribleSet;
                        use triblespace_core::macros::{find, pattern};

                        let cap_blob: Blob<SimpleArchive> = Blob::new(cap_bytes.clone());
                        let sig_blob: Blob<SimpleArchive> = Blob::new(sig_bytes.clone());
                        let cap_hash: RawHash = *blake3::hash(&cap_bytes).as_bytes();
                        let sig_hash: RawHash = *blake3::hash(&sig_bytes).as_bytes();
                        let sig_handle: Inline<Handle<SimpleArchive>> =
                            Inline::new(sig_hash);

                        // Cheap DoS guard before any swarm work: the
                        // cap's declared `cap_issuer` must equal the
                        // TLS-verified pubkey of whoever just dialed
                        // us. The auth-handshake ALPN is open to
                        // unauthenticated peers, so without this gate
                        // a stranger could ship a cap with our subject
                        // + a `cap_parent` pointing at random hashes,
                        // and we'd burn DHT lookups chasing chain
                        // blobs that will never verify. The check
                        // costs one `find!` against the leaf cap
                        // blob.
                        let declared_issuer = if let Ok(cap_set) =
                            TribleSet::try_from_blob(cap_blob.clone())
                        {
                            find!(
                                (issuer: ed25519_dalek::VerifyingKey),
                                pattern!(&cap_set, [{
                                    triblespace_core::repo::capability::cap_issuer: ?issuer,
                                }])
                            )
                            .next()
                            .map(|(k,)| k)
                        } else {
                            None
                        };
                        match declared_issuer {
                            Some(issuer) if issuer.to_bytes() == peer_pubkey_bytes => {}
                            Some(issuer) => {
                                warn!(
                                    declared_issuer = %hex::encode(&issuer.to_bytes()[..4]),
                                    dialer = %hex::encode(&peer_pubkey_bytes[..4]),
                                    "OP_DELIVER_CAP: cap_issuer doesn't match TLS dialer; rejecting",
                                );
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_REJECTED,
                                )
                                .await;
                                continue;
                            }
                            None => {
                                warn!("OP_DELIVER_CAP: cap blob malformed or missing cap_issuer; rejecting");
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_MALFORMED,
                                )
                                .await;
                                continue;
                            }
                        }

                        // Verify-with-swarm-fetch: try local first, then
                        // pull missing chain blobs via the same
                        // DHT-routed pool path OP_AUTH uses. The dialer
                        // is the immediate issuer and almost certainly
                        // has the parent cap, but for 3+ hop chains the
                        // intermediate cap might live elsewhere — DHT
                        // provider lookup finds them either way.
                        let verify_once = |fetched: &std::collections::BTreeMap<RawHash, anybytes::Bytes>| {
                            let snap_for_fetch = snapshot.clone();
                            let fetched_for_lookup = fetched.clone();
                            let cap_blob_for_fetch = cap_blob.clone();
                            let sig_blob_for_fetch = sig_blob.clone();
                            triblespace_core::repo::capability::verify_chain(
                                team_root,
                                sig_handle,
                                our_pubkey,
                                move |h: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                                    if h.raw == cap_hash {
                                        return Some(cap_blob_for_fetch.clone());
                                    }
                                    if h.raw == sig_hash {
                                        return Some(sig_blob_for_fetch.clone());
                                    }
                                    if let Some(bytes) = snap_for_fetch
                                        .lock()
                                        .unwrap()
                                        .as_ref()
                                        .and_then(|s| s.get_blob(&h.raw))
                                    {
                                        return Some(Blob::new(anybytes::Bytes::from_source(bytes)));
                                    }
                                    let bytes = fetched_for_lookup.get(&h.raw)?.clone();
                                    Some(Blob::new(bytes))
                                },
                            )
                        };

                        let mut fetched: std::collections::BTreeMap<RawHash, anybytes::Bytes> =
                            std::collections::BTreeMap::new();
                        let mut result = verify_once(&fetched);

                        if matches!(
                            result,
                            Err(triblespace_core::repo::capability::VerifyError::Fetch),
                        ) {
                            debug!(
                                sig = %hex::encode(&sig_hash[..4]),
                                "OP_DELIVER_CAP: chain incomplete locally, swarm-fetching",
                            );

                            // Use the just-received `sig_hash` as the
                            // OP_AUTH credential for the swarm-fetch
                            // — for both first-time delivery and
                            // renewals. The new cap is by definition
                            // the one we're going to be using going
                            // forward; the prior `self_cap` is at
                            // best redundant and at worst
                            // already-expired. The dialer-equals-
                            // issuer precheck above already
                            // established that the cap was actually
                            // signed by this dialer, so they trivially
                            // accept it on AUTH (they have its
                            // chain), and the remote's own OP_AUTH
                            // path validates against team_root for
                            // anyone deeper.
                            fetched = swarm_fetch_chain(
                                &transport, peer_pubkey_bytes, &sig_hash,
                                &sig_hash, &pool,
                            )
                            .await;
                            debug!(blobs = fetched.len(), "swarm-fetched chain blobs");
                            result = verify_once(&fetched);
                        }

                        match result {
                            Ok(_verified) => {
                                debug!(
                                    sig = %hex::encode(&sig_hash[..4]),
                                    issuer = %hex::encode(&peer_pubkey_bytes[..4]),
                                    "OP_DELIVER_CAP: chain verified; absorbing",
                                );
                                // Emit Blob events for everything the
                                // verify needed — the in-band leaf
                                // pair + every swarm-fetched parent.
                                // mpsc preserves order so the Peer
                                // thread sees these before the
                                // CapDelivered marker that triggers
                                // pinning.
                                let _ = events.send(NetEvent::Blob(cap_bytes.clone()));
                                let _ = events.send(NetEvent::Blob(sig_bytes.clone()));
                                for (_, bytes) in std::mem::take(&mut fetched) {
                                    let _ = events.send(NetEvent::Blob(bytes));
                                }
                                let _ = events.send(NetEvent::CapDelivered {
                                    issuer: peer_pubkey_bytes,
                                    cap_bytes,
                                    sig_bytes,
                                });
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_OK,
                                )
                                .await;
                            }
                            Err(e) => {
                                warn!(
                                    error = ?e,
                                    sig = %hex::encode(&sig_hash[..4]),
                                    "OP_DELIVER_CAP: chain verify failed; rejecting",
                                );
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_REJECTED,
                                )
                                .await;
                            }
                        }
                    }
                    Ok(None) => {
                        let _ = crate::handshake::respond(
                            &mut send,
                            crate::handshake::STATUS_MALFORMED,
                        )
                        .await;
                    }
                    Err(e) => {
                        debug!(error = %e, "handshake decode error; rejecting");
                        let _ = crate::handshake::respond(
                            &mut send,
                            crate::handshake::STATUS_MALFORMED,
                        )
                        .await;
                    }
                }
            }
        }
        .instrument(span)
        .await;
    }
}

impl<T: Transport> SnapshotHandler<T> {
    async fn handle(&self, connection: T::Conn) {
        let snap = self.snapshot.clone();
        let team_root = self.team_root;
        let transport = self.transport.clone();
        let self_cap = self.self_cap;
        let events = self.events.clone();
        let pool = self.pool.clone();

        let peer_id: PeerId = connection.remote_id();
        let span = info_span!(
            "connection",
            peer = %hex::encode(&peer_id[..4]),
            alpn = %String::from_utf8_lossy(PILE_SYNC_ALPN),
        );

        async move {
            info!("connection accepted");

            // The connecting peer's verified ed25519 identity from
            // the transport's TLS layer.
            let peer_pubkey = match ed25519_dalek::VerifyingKey::from_bytes(&peer_id) {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "peer pubkey parse failed; closing");
                    return;
                }
            };

            // Per-connection auth state. The first stream is processed
            // synchronously below and must set this before any later stream is
            // accepted. Subsequent streams may then execute concurrently while
            // each snapshots the same verified capability.
            let auth_state: Arc<
                tokio::sync::RwLock<Option<triblespace_core::repo::capability::VerifiedCapability>>,
            > = Arc::new(tokio::sync::RwLock::new(None));

            let Some((mut first_send, mut first_recv)) = connection.accept_bi().await else {
                debug!("connection closed before mandatory OP_AUTH stream");
                return;
            };
            if let Err(e) = serve_stream(
                &snap,
                team_root,
                peer_pubkey,
                auth_state.clone(),
                true,
                &transport,
                &self_cap,
                &events,
                &pool,
                &mut first_send,
                &mut first_recv,
            )
            .await
            {
                error!(error = %e, "first-stream authentication failed");
            }
            let _ = first_send.shutdown().await;
            if auth_state.read().await.is_none() {
                debug!("mandatory first-stream OP_AUTH did not authenticate; closing connection");
                connection.close(0, b"authentication required");
                return;
            }

            loop {
                let Some((mut send, mut recv)) = connection.accept_bi().await else {
                    debug!("accept_bi ended; connection closing");
                    break;
                };
                let snap = snap.clone();
                let auth_state = auth_state.clone();
                let transport = transport.clone();
                let events = events.clone();
                let pool = pool.clone();
                tokio::spawn(
                    async move {
                        if let Err(e) = serve_stream(
                            &snap,
                            team_root,
                            peer_pubkey,
                            auth_state,
                            false,
                            &transport,
                            &self_cap,
                            &events,
                            &pool,
                            &mut send,
                            &mut recv,
                        )
                        .await
                        {
                            error!(error = %e, "stream handler error");
                        }
                        let _ = send.shutdown().await;
                    }
                    .in_current_span(),
                );
            }
        }
        .instrument(span)
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_stream<T: Transport>(
    snap_arc: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    team_root: ed25519_dalek::VerifyingKey,
    peer_pubkey: ed25519_dalek::VerifyingKey,
    auth_state: Arc<
        tokio::sync::RwLock<Option<triblespace_core::repo::capability::VerifiedCapability>>,
    >,
    auth_allowed: bool,
    t: &T,
    self_cap: &RawHash,
    events: &mpsc::Sender<NetEvent>,
    pool: &SharedPool<T::Conn>,
    send: &mut <T::Conn as Conn>::SendHalf,
    recv: &mut <T::Conn as Conn>::RecvHalf,
) -> anyhow::Result<()> {
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::inline::Inline;
    use triblespace_core::inline::encodings::hash::Handle;

    let op = recv_u8(recv).await?;
    let span = debug_span!("stream", op = op_name(op));
    let _enter = span.enter();

    if op == OP_AUTH {
        if !auth_allowed {
            debug!("OP_AUTH is only valid on the first stream; rejecting re-authentication");
            send_u8(send, AUTH_REJECTED).await?;
            return Ok(());
        }
        let cap_handle_raw = recv_hash(recv).await?;
        debug!(cap_handle = %hex::encode(&cap_handle_raw[..4]), "auth: cap handle received");
        let cap_handle: Inline<Handle<SimpleArchive>> = Inline::new(cap_handle_raw);

        // Brief sync read inside async — guard is dropped before any
        // .await runs so this never blocks an async worker.
        // First-pass verify with local-only lookup. The common case is
        // "we already have the whole chain"; only retry with a swarm
        // fetch on the specific "missing blob" failure mode.
        let verify_once = |fetched: &std::collections::BTreeMap<RawHash, anybytes::Bytes>| {
            let snap_for_fetch = snap_arc.clone();
            let fetched_for_lookup = fetched.clone();
            triblespace_core::repo::capability::verify_chain(
                team_root,
                cap_handle,
                peer_pubkey,
                move |h: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
                    if let Some(bytes) = snap_for_fetch
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(|s| s.get_blob(&h.raw))
                    {
                        return Some(Blob::new(anybytes::Bytes::from_source(bytes)));
                    }
                    let bytes = fetched_for_lookup.get(&h.raw)?.clone();
                    Some(Blob::new(bytes))
                },
            )
        };

        let mut fetched: std::collections::BTreeMap<RawHash, anybytes::Bytes> =
            std::collections::BTreeMap::new();
        let mut result = verify_once(&fetched);

        // Swarm fetch + retry on missing-blob. Caps are orphan blobs
        // (not reachable from any branch HEAD), so they don't ride
        // along with normal sync. On first auth from a peer whose
        // chain we haven't cached, this derives a conservative hinted subgraph
        // from content-verified blobs and pulls it into a local map. This fallback
        // requires some already-authenticatable provider of the missing chain;
        // two cold peers that each know only their own chain need a separate
        // bootstrap presentation path.
        if matches!(
            result,
            Err(triblespace_core::repo::capability::VerifyError::Fetch),
        ) {
            debug!(
                cap_handle = %hex::encode(&cap_handle_raw[..4]),
                "auth: chain incomplete locally, swarm-fetching",
            );
            let publisher: PeerId = peer_pubkey.to_bytes();
            fetched = swarm_fetch_chain(t, publisher, &cap_handle_raw, self_cap, pool).await;
            debug!(blobs = fetched.len(), "swarm-fetched chain blobs");
            result = verify_once(&fetched);
        }

        match result {
            Ok(verified) => {
                let granted = verified.granted_branches().map(|s| s.len()).unwrap_or(0);
                let unrestricted = verified.granted_branches().is_none();
                info!(branches = granted, unrestricted = unrestricted, "auth ok");
                // Cache the swarm-fetched blobs into the local store so
                // the next AUTH involving the same chain finds them
                // locally. mpsc preserves order; child-before-parent
                // ordering doesn't matter here because the chain is
                // already self-consistent (every parent referenced by
                // every fetched cap is also in `fetched`).
                for (_, bytes) in std::mem::take(&mut fetched) {
                    let _ = events.send(NetEvent::Blob(bytes));
                }
                // Tell the Peer thread that this remote authed with
                // `cap_handle_raw`. If the Peer issued a cap to this
                // subject and `cap_handle_raw` matches the policy
                // entry's `latest_sig`, the Peer marks the entry as
                // delivered (the subject has the cap and can use it).
                let _ = events.send(NetEvent::CapDeliveryConfirmed {
                    subject: peer_pubkey.to_bytes(),
                    sig_handle: cap_handle_raw,
                });
                *auth_state.write().await = Some(verified);
                send_u8(send, AUTH_OK).await?;
            }
            Err(e) => {
                warn!(error = ?e, "auth rejected");
                send_u8(send, AUTH_REJECTED).await?;
            }
        }
        return Ok(());
    }

    // All other ops require a verified cap on the connection. Snapshot
    // the auth state once so the scope gate sees a stable view of the
    // verified cap for the rest of this stream's lifetime.
    let verified = match auth_state.read().await.clone() {
        Some(v) => v,
        None => {
            // Not authenticated. Close the stream silently — the client
            // should have presented OP_AUTH first.
            debug!("op without prior OP_AUTH on connection; closing stream");
            return Ok(());
        }
    };
    // Blob scope gate: `OP_GET_BLOB` and the untrusted `OP_CHILDREN` hint are
    // filtered by blob-graph reachability from legacy mutable-pin roots allowed
    // by the verified capability. The retired OP_LIST/OP_HEAD operations no
    // longer expose those roots over RPC. Unrestricted caps
    // (`granted_branches() == None`) skip the reachability filter.
    //
    // Reachability is recomputed per operation for simplicity; a per-connection
    // cache would be the obvious next optimisation.

    match op {
        OP_GET_BLOB => {
            let hash = recv_hash(recv).await?;
            let in_scope_flag;
            let data = {
                let guard = snap_arc.lock().unwrap();
                let scope_ok = guard
                    .as_ref()
                    .map(|snap| blob_in_scope(snap.as_ref(), &verified, &hash))
                    .unwrap_or(false);
                in_scope_flag = scope_ok;
                guard.as_ref().and_then(|snap| {
                    if !scope_ok {
                        return None;
                    }
                    snap.get_blob(&hash)
                })
            };
            match data {
                Some(data) => {
                    debug!(hash = %hex::encode(&hash[..4]), bytes = data.len(), "OP_GET_BLOB served");
                    send_u64_be(send, data.len() as u64).await?;
                    send.write_all(&data)
                        .await
                        .map_err(|e| anyhow::anyhow!("send: {e}"))?;
                }
                None => {
                    if !in_scope_flag {
                        warn!(hash = %hex::encode(&hash[..4]), "OP_GET_BLOB denied: out of scope");
                    } else {
                        debug!(hash = %hex::encode(&hash[..4]), "OP_GET_BLOB miss: blob not present");
                    }
                    send_u64_be(send, u64::MAX).await?;
                }
            }
        }

        OP_CHILDREN => {
            let parent_hash = recv_hash(recv).await?;
            let (parent_in_scope, children) = {
                let guard = snap_arc.lock().unwrap();
                match guard.as_ref() {
                    None => (false, Vec::new()),
                    Some(snap) => {
                        let reachable = reachable_set_for(snap.as_ref(), &verified);
                        let in_scope = |hash: &RawHash| -> bool {
                            if !snap.has_blob(hash) {
                                return false;
                            }
                            match &reachable {
                                None => verified.grants_read(),
                                Some(set) => set.contains(hash),
                            }
                        };
                        if !in_scope(&parent_hash) {
                            (false, Vec::new())
                        } else {
                            let children = snap
                                .get_blob(&parent_hash)
                                .map(|bytes| {
                                    potential_child_hashes(&bytes)
                                        .filter(|candidate| in_scope(candidate))
                                        .take(MAX_CHILD_HINTS)
                                        .collect()
                                })
                                .unwrap_or_default();
                            (true, children)
                        }
                    }
                }
            };
            if !parent_in_scope {
                warn!(parent = %hex::encode(&parent_hash[..4]), "OP_CHILDREN denied or absent");
            } else {
                debug!(parent = %hex::encode(&parent_hash[..4]), n = children.len(), "OP_CHILDREN hints served");
            }
            for hash in &children {
                send_hash(send, hash).await?;
            }
            send_hash(send, &NIL_HASH).await?;
        }

        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracking_key(pin: u8, publisher: u8) -> TrackingKey {
        TrackingKey {
            pin: [pin; 16],
            publisher: [publisher; 32],
        }
    }

    fn started(outcome: BeginOutcome) -> TrackingAttempt {
        let BeginOutcome::Start(attempt) = outcome else {
            panic!("expected Start, got {outcome:?}");
        };
        attempt
    }

    #[test]
    fn older_success_after_newer_success_is_stale() {
        let mut state = RetryState::default();
        let key = tracking_key(1, 7);
        let now = crate::clock::mono_now();
        let older = started(state.begin(key, [10; 32], [7; 32], now));
        let newer = started(state.begin(key, [11; 32], [7; 32], now));

        assert!(state.complete_success(&newer, now));
        assert!(!state.complete_success(&older, now));
        assert!(state.active.is_empty());
        assert!(state.pending.is_empty());
    }

    #[test]
    fn older_failure_after_newer_success_cannot_resurrect_retry() {
        let mut state = RetryState::default();
        let key = tracking_key(1, 7);
        let now = crate::clock::mono_now();
        let older = started(state.begin(key, [10; 32], [7; 32], now));
        let newer = started(state.begin(key, [11; 32], [7; 32], now));

        assert!(state.complete_success(&newer, now));
        assert_eq!(state.complete_failure(older, now), FailureOutcome::Stale);
        assert!(state.active.is_empty());
        assert!(state.pending.is_empty());
    }

    #[test]
    fn same_remote_id_from_two_publishers_has_isolated_retries() {
        let mut state = RetryState::default();
        let key_a = tracking_key(1, 7);
        let key_b = tracking_key(1, 9);
        let now = crate::clock::mono_now();
        let attempt_a = started(state.begin(key_a, [10; 32], [7; 32], now));
        let attempt_b = started(state.begin(key_b, [20; 32], [9; 32], now));

        assert_eq!(
            state.complete_failure(attempt_a, now),
            FailureOutcome::Queued
        );
        assert_eq!(
            state.complete_failure(attempt_b, now),
            FailureOutcome::Queued
        );
        assert!(state.pending.contains_key(&key_a));
        assert!(state.pending.contains_key(&key_b));

        let newer_b = started(state.begin(key_b, [21; 32], [9; 32], now));
        assert!(state.pending.contains_key(&key_a));
        assert!(!state.pending.contains_key(&key_b));
        assert_eq!(
            state.complete_failure(attempt_b, now),
            FailureOutcome::Stale
        );
        assert!(state.complete_success(&newer_b, now));
        assert!(state.pending.contains_key(&key_a));
        assert!(state.active.contains_key(&key_a));
    }

    #[test]
    fn in_flight_retry_is_stale_after_new_observation() {
        let mut state = RetryState::default();
        let key = tracking_key(1, 7);
        let now = crate::clock::mono_now();
        let initial = started(state.begin(key, [10; 32], [7; 32], now));
        assert_eq!(state.complete_failure(initial, now), FailureOutcome::Queued);
        let retry = state
            .take_one_due(now + crate::RETRY_BACKOFF_CAP)
            .expect("retry becomes due");

        let newer = started(state.begin(key, [11; 32], [7; 32], now));
        assert!(!state.complete_success(&retry, now));
        assert_eq!(state.complete_failure(retry, now), FailureOutcome::Stale);
        assert!(state.complete_success(&newer, now));
        assert!(state.pending.is_empty());
    }

    #[test]
    fn same_head_in_flight_is_coalesced() {
        let mut state = RetryState::default();
        let now = crate::clock::mono_now();
        let key = tracking_key(1, 7);
        let first = started(state.begin(key, [10; 32], [7; 32], now));

        assert_eq!(
            state.begin(key, first.metadata_head, [7; 32], now),
            BeginOutcome::Coalesced
        );
        assert_eq!(state.active.len(), 1);
        assert!(state.pending.is_empty());
        assert!(state.is_current(&first));
    }

    #[test]
    fn same_head_pending_retry_restarts_immediately() {
        let mut state = RetryState::default();
        let now = crate::clock::mono_now();
        let key = tracking_key(1, 7);
        let first = started(state.begin(key, [10; 32], [7; 32], now));
        assert_eq!(state.complete_failure(first, now), FailureOutcome::Queued);

        let promoted = started(state.begin(key, first.metadata_head, [7; 32], now));
        assert_eq!(promoted.generation, first.generation);
        assert_eq!(promoted.attempt, 1);
        assert!(state.pending.is_empty());
    }

    #[test]
    fn capacity_rejects_new_keys_but_allows_existing_key_update() {
        let mut state = RetryState::with_limits(2, std::time::Duration::from_secs(10));
        let now = crate::clock::mono_now();
        let key_a = tracking_key(1, 7);
        let key_b = tracking_key(2, 7);
        let key_c = tracking_key(3, 7);
        let _ = started(state.begin(key_a, [10; 32], [7; 32], now));
        let _ = started(state.begin(key_b, [20; 32], [7; 32], now));

        assert_eq!(
            state.begin(key_c, [30; 32], [7; 32], now),
            BeginOutcome::AtCapacity
        );
        let replacement = started(state.begin(key_a, [11; 32], [7; 32], now));
        assert_eq!(replacement.metadata_head, [11; 32]);
        assert_eq!(state.active.len(), 2);
    }

    #[test]
    fn replay_and_head_churn_do_not_extend_fixed_lease() {
        let lease = std::time::Duration::from_secs(10);
        let mut state = RetryState::with_limits(2, lease);
        let admitted = crate::clock::mono_now();
        let key = tracking_key(1, 7);
        let first = started(state.begin(key, [10; 32], [7; 32], admitted));
        let near_expiry = admitted + std::time::Duration::from_secs(9);
        assert_eq!(
            state.begin(key, first.metadata_head, [7; 32], near_expiry),
            BeginOutcome::Coalesced
        );
        let churned = started(state.begin(key, [11; 32], [7; 32], near_expiry));

        state.prune_expired(admitted + lease);
        assert!(state.active.is_empty());
        assert!(!state.is_current(&churned));
        let readmitted = started(state.begin(key, [11; 32], [7; 32], admitted + lease));
        assert_ne!(readmitted.generation, churned.generation);
    }

    #[test]
    fn expired_pending_retry_is_never_due() {
        let lease = std::time::Duration::from_millis(500);
        let mut state = RetryState::with_limits(1, lease);
        let now = crate::clock::mono_now();
        let key = tracking_key(1, 7);
        let first = started(state.begin(key, [10; 32], [7; 32], now));
        assert_eq!(state.complete_failure(first, now), FailureOutcome::Queued);

        assert!(state.take_one_due(now + lease).is_none());
        assert!(state.active.is_empty());
        assert!(state.pending.is_empty());
    }

    #[test]
    fn invalid_claimed_publisher_normalizes_to_authenticated_relayer() {
        let invalid = (0u8..=u8::MAX)
            .map(|byte| [byte; 32])
            .find(|candidate| ed25519_dalek::VerifyingKey::from_bytes(candidate).is_err())
            .expect("some 32-byte strings are not compressed Edwards points");
        let relayer = SigningKey::from_bytes(&[77; 32]).verifying_key().to_bytes();
        assert_eq!(normalize_publisher(invalid, relayer), relayer);

        let valid = SigningKey::from_bytes(&[78; 32]).verifying_key().to_bytes();
        assert_eq!(normalize_publisher(valid, relayer), valid);
    }

    #[test]
    fn hint_walk_budget_charges_only_completed_network_fetches() {
        let limits = HintWalkLimits {
            max_blobs: 2,
            max_bytes: 5,
        };
        let mut count_limited = HintWalkBudget::new(limits);
        count_limited.admit_fetch().unwrap();
        count_limited.record_fetch(2).unwrap();
        count_limited.admit_fetch().unwrap();
        count_limited.record_fetch(3).unwrap();
        assert!(count_limited.admit_fetch().is_err());

        let mut byte_limited = HintWalkBudget::new(limits);
        byte_limited.admit_fetch().unwrap();
        byte_limited.record_fetch(4).unwrap();
        byte_limited.admit_fetch().unwrap();
        byte_limited.record_fetch(2).unwrap();
        assert!(byte_limited.admit_fetch().is_err());
    }

    #[cfg(feature = "sim")]
    #[tokio::test(start_paused = true)]
    async fn hint_walk_slices_and_timeouts_retain_verified_progress() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use crate::transport::sim::{SimConfig, SimNet};

        let child_bytes = b"leaf".to_vec();
        let child = *blake3::hash(&child_bytes).as_bytes();
        let root_bytes = child.to_vec();
        let root = *blake3::hash(&root_bytes).as_bytes();
        let publisher = [41; 32];
        let client = [42; 32];
        let self_cap = [43; 32];
        let net = SimNet::new(
            0x510C_E001,
            SimConfig {
                latency: std::time::Duration::ZERO..std::time::Duration::from_nanos(1),
                ..SimConfig::default()
            },
        );
        let mut server = net.join(publisher, false);
        let client = net.join(client, false).transport;
        let stall_next_children = Arc::new(AtomicBool::new(false));
        let fail_next_children = Arc::new(AtomicBool::new(false));
        let server_stall = stall_next_children.clone();
        let server_fail = fail_next_children.clone();
        tokio::spawn(async move {
            while let Some(incoming) = server.incoming.recv().await {
                if incoming.alpn != PILE_SYNC_ALPN {
                    continue;
                }
                let connection = incoming.conn;
                let root_bytes = root_bytes.clone();
                let child_bytes = child_bytes.clone();
                let stall = server_stall.clone();
                let fail = server_fail.clone();
                tokio::spawn(async move {
                    let Some((mut send, mut recv)) = connection.accept_bi().await else {
                        return;
                    };
                    if recv_u8(&mut recv).await.ok() != Some(OP_AUTH) {
                        return;
                    }
                    let _ = recv_hash(&mut recv).await;
                    let _ = send_u8(&mut send, AUTH_OK).await;
                    let _ = send.shutdown().await;

                    while let Some((mut send, mut recv)) = connection.accept_bi().await {
                        match recv_u8(&mut recv).await {
                            Ok(OP_GET_BLOB) => {
                                let Ok(requested) = recv_hash(&mut recv).await else {
                                    break;
                                };
                                let data = if requested == root {
                                    Some(&root_bytes)
                                } else if requested == child {
                                    Some(&child_bytes)
                                } else {
                                    None
                                };
                                if let Some(data) = data {
                                    let _ = send_u64_be(&mut send, data.len() as u64).await;
                                    let _ = send.write_all(data).await;
                                } else {
                                    let _ = send_u64_be(&mut send, u64::MAX).await;
                                }
                                let _ = send.shutdown().await;
                            }
                            Ok(OP_CHILDREN) => {
                                let Ok(parent) = recv_hash(&mut recv).await else {
                                    break;
                                };
                                if parent == root && fail.swap(false, Ordering::SeqCst) {
                                    connection.close(0, b"injected CHILDREN failure");
                                    break;
                                }
                                if parent == root && stall.swap(false, Ordering::SeqCst) {
                                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                }
                                if parent == root {
                                    let _ = send_hash(&mut send, &child).await;
                                }
                                let _ = send_hash(&mut send, &NIL_HASH).await;
                                let _ = send.shutdown().await;
                            }
                            _ => break,
                        }
                    }
                });
            }
        });

        // One fetched blob per slice: the first slice lands the root but may
        // not claim completion; the next sees that durable prefix for free and
        // reaches the child.
        let local = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let pool = new_shared_pool();
        let first_local = local.clone();
        let first_sink = local.clone();
        let first = fetch_hinted_subgraph(
            &client,
            publisher,
            &root,
            &pool,
            &self_cap,
            move |hash| first_local.lock().unwrap().get(hash).cloned(),
            move |hash, bytes| {
                first_sink.lock().unwrap().insert(hash, bytes.to_vec());
            },
            HintWalkLimits {
                max_blobs: 1,
                max_bytes: 1024,
            },
        )
        .await;
        assert!(first.is_err(), "an exhausted slice must not admit its Head");
        assert_eq!(
            local.lock().unwrap().keys().copied().collect::<Vec<_>>(),
            vec![root]
        );

        let second_local = local.clone();
        let second_sink = local.clone();
        let second = fetch_hinted_subgraph(
            &client,
            publisher,
            &root,
            &pool,
            &self_cap,
            move |hash| second_local.lock().unwrap().get(hash).cloned(),
            move |hash, bytes| {
                second_sink.lock().unwrap().insert(hash, bytes.to_vec());
            },
            HintWalkLimits {
                max_blobs: 1,
                max_bytes: 1024,
            },
        )
        .await
        .expect("the next slice completes from the retained local prefix");
        assert_eq!(second.blobs.len(), 2);
        assert!(local.lock().unwrap().contains_key(&child));

        // A total CHILDREN transport failure is distinct from an answered
        // empty hint set: it retains the verified root but cannot admit
        // completion. A later attempt can then make progress from that root.
        let failed_local = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let failed_pool = new_shared_pool();
        fail_next_children.store(true, Ordering::SeqCst);
        let lookup = failed_local.clone();
        let sink = failed_local.clone();
        let failed = fetch_hinted_subgraph(
            &client,
            publisher,
            &root,
            &failed_pool,
            &self_cap,
            move |hash| lookup.lock().unwrap().get(hash).cloned(),
            move |hash, bytes| {
                sink.lock().unwrap().insert(hash, bytes.to_vec());
            },
            HINT_WALK_LIMITS,
        )
        .await;
        assert!(failed.is_err(), "an unanswered hint request is retryable");
        assert_eq!(
            failed_local
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![root]
        );

        let retry_pool = new_shared_pool();
        let lookup = failed_local.clone();
        let sink = failed_local.clone();
        let recovered = fetch_hinted_subgraph(
            &client,
            publisher,
            &root,
            &retry_pool,
            &self_cap,
            move |hash| lookup.lock().unwrap().get(hash).cloned(),
            move |hash, bytes| {
                sink.lock().unwrap().insert(hash, bytes.to_vec());
            },
            HINT_WALK_LIMITS,
        )
        .await
        .expect("a later answered hint request completes from retained progress");
        assert_eq!(recovered.blobs.len(), 2);

        // Cancellation has the same monotone behavior: a callback-fired blob
        // outlives the cancelled future, while completion remains gated.
        let timed_local = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let timed_pool = new_shared_pool();
        stall_next_children.store(true, Ordering::SeqCst);
        let lookup = timed_local.clone();
        let sink = timed_local.clone();
        let timed = tokio::time::timeout(
            std::time::Duration::from_millis(1),
            fetch_hinted_subgraph(
                &client,
                publisher,
                &root,
                &timed_pool,
                &self_cap,
                move |hash| lookup.lock().unwrap().get(hash).cloned(),
                move |hash, bytes| {
                    sink.lock().unwrap().insert(hash, bytes.to_vec());
                },
                HINT_WALK_LIMITS,
            ),
        )
        .await;
        assert!(
            timed.is_err(),
            "the deliberately stalled walk must time out"
        );
        assert_eq!(
            timed_local
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![root],
            "the verified root survives cancellation without admitting completion"
        );

        // A fresh pool avoids reusing the cancelled in-flight stream. The
        // server's one-shot stall has cleared, so the retry advances.
        let retry_pool = new_shared_pool();
        let lookup = timed_local.clone();
        let sink = timed_local.clone();
        let retry = fetch_hinted_subgraph(
            &client,
            publisher,
            &root,
            &retry_pool,
            &self_cap,
            move |hash| lookup.lock().unwrap().get(hash).cloned(),
            move |hash, bytes| {
                sink.lock().unwrap().insert(hash, bytes.to_vec());
            },
            HINT_WALK_LIMITS,
        )
        .await
        .expect("retry completes after consuming timeout-retained progress");
        assert_eq!(retry.blobs.len(), 2);
        assert!(timed_local.lock().unwrap().contains_key(&child));
    }

    #[tokio::test]
    async fn tracking_permit_is_acquired_before_work_can_spawn() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots
            .clone()
            .try_acquire_owned()
            .expect("first walk admitted");
        assert!(
            slots.clone().try_acquire_owned().is_err(),
            "a second walk cannot even be admitted while the permit is held"
        );
        drop(permit);
        assert!(slots.try_acquire_owned().is_ok());
    }

    #[test]
    fn dht_fallbacks_exclude_self_publisher_and_duplicates() {
        let me = [1; 32];
        let publisher = [2; 32];
        let cache_a = [3; 32];
        let cache_b = [4; 32];
        assert_eq!(
            dht_fallback_candidates(me, publisher, [me, cache_a, publisher, cache_a, cache_b],),
            vec![cache_a, cache_b]
        );
        assert_eq!(
            dht_fallback_candidates(me, me, [me, cache_a, cache_a]),
            vec![cache_a]
        );

        let many = (10u8..30).map(|byte| [byte; 32]);
        assert_eq!(
            dht_fallback_candidates(me, publisher, many),
            (10u8..18).map(|byte| [byte; 32]).collect::<Vec<_>>(),
            "fallback discovery order is preserved and dial fanout is capped"
        );
    }
}

/// Build the reachable set for the given verified cap once. Returns
/// `None` if the cap is unrestricted (i.e. every present blob is in
/// scope — caller short-circuits to `snap.has_blob` checks).
/// Returns `Some(set)` for legacy-id-restricted caps; the BFS walks
/// from each allowed mutable pin following conservative 32-byte child
/// candidates that exist in the local snapshot.
fn reachable_set_for(
    snap: &dyn AnySnapshot,
    verified: &triblespace_core::repo::capability::VerifiedCapability,
) -> Option<HashSet<RawHash>> {
    if verified.granted_branches().is_none() {
        // Unrestricted cap: every blob present in the snapshot is in
        // scope. The cap may still lack read permission entirely; callers
        // cross-check `verified.grants_read()` before serving a blob.
        return None;
    }

    let pins = snap.pins();
    let mut frontier: Vec<RawHash> = pins
        .iter()
        .filter_map(|pin| {
            triblespace_core::id::Id::new(*pin)
                .filter(|id| verified.grants_read_on(id))
                .and_then(|_| pins.get(pin).map(|h| h.raw))
        })
        .collect();
    let mut reachable: HashSet<RawHash> = HashSet::new();
    while let Some(h) = frontier.pop() {
        if !reachable.insert(h) {
            continue;
        }
        if let Some(data) = snap.get_blob(&h) {
            for chunk in data.chunks(32) {
                if chunk.len() == 32 {
                    let mut child = [0u8; 32];
                    child.copy_from_slice(chunk);
                    if snap.has_blob(&child) && !reachable.contains(&child) {
                        frontier.push(child);
                    }
                }
            }
        }
    }
    Some(reachable)
}

/// Returns `true` if `hash` is reachable (transitively, via 32-byte-chunk
/// children references) from at least one legacy mutable pin the `verified`
/// cap grants read access on. Unrestricted caps short-circuit to `true` for
/// every hash present in the snapshot.
///
/// Convenience wrapper over [`reachable_set_for`] for one requested hash.
fn blob_in_scope(
    snap: &dyn AnySnapshot,
    verified: &triblespace_core::repo::capability::VerifiedCapability,
    hash: &RawHash,
) -> bool {
    if !snap.has_blob(hash) {
        return false;
    }
    match reachable_set_for(snap, verified) {
        None => verified.grants_read(),
        Some(set) => set.contains(hash),
    }
}
