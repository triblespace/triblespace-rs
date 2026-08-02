//! Network thread: spawns the iroh endpoint, DHT, and protocol server.
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
use crate::transport::{Conn, Harness, PeerId, Transport};
use tokio::io::AsyncWriteExt;

type LocalPinId = [u8; 16];

fn op_name(op: u8) -> &'static str {
    match op {
        OP_AUTH => "AUTH",
        OP_GET_BLOB => "GET_BLOB",
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
/// `Default` impl — auth is mandatory in protocol v5 so every peer
/// construction site must explicitly choose a team root. For solo
/// workflows the convention is `team_root = signing_key.verifying_key()`
/// (the user is the team root and the founder of a team-of-one);
/// see the `Peer` struct's doctest for the full pattern.
pub struct PeerConfig {
    /// Bootstrap peers for the DHT.
    /// `EndpointAddr` here carries only an `EndpointId`; iroh's
    /// standard discovery (pkarr / DNS via `presets::N0`) resolves
    /// the actual relay URL and direct addresses at dial time.
    pub peers: Vec<EndpointAddr>,
    /// The team root public key — verifies all incoming capability
    /// chains. Every connection's first stream must present a cap that
    /// chains back to this key. See `triblespace_core::repo::capability`.
    pub team_root: ed25519_dalek::VerifyingKey,
    /// This node's own capability sig handle. Presented to remote peers
    /// as the first stream on every outgoing connection so they can
    /// authorise us. Required — protocol v5 has mandatory auth on both
    /// directions of a connection.
    pub self_cap: RawHash,
}

// No `Default` impl: every PeerConfig must specify a team root because
// auth is mandatory in protocol v5. For a single-user OSS deployment
// the convention is `team_root = signing_key.verifying_key()` (the user
// is the team root and the founder of a team-of-one).

/// Snapshot of store state for serving protocol requests.
pub struct StoreSnapshot<R> {
    pub reader: R,
    /// Positively identified legacy mutable-pin roots only. Generic PinStore
    /// entries (policy, retention, fetch wants) are deliberately absent.
    legacy_heads: HashMap<LocalPinId, RawHash>,
}

impl StoreSnapshot<()> {
    pub fn from_store<S>(store: &mut S) -> Option<StoreSnapshot<S::Reader>>
    where
        S: triblespace_core::repo::BlobStore + triblespace_core::repo::PinStore,
    {
        let pins = store.pin_snapshot().ok()?;
        let reader = store.reader().ok()?;
        let legacy_heads = pins
            .iter()
            .filter_map(|raw_pin| {
                let pin_id = triblespace_core::id::Id::new(*raw_pin)?;
                let metadata_head = *pins.get(raw_pin)?;
                crate::legacy::is_legacy_pin_metadata(&reader, pin_id, metadata_head)
                    .then_some((*raw_pin, metadata_head.raw))
            })
            .collect();
        Some(StoreSnapshot {
            reader,
            legacy_heads,
        })
    }
}

/// Type-erased snapshot for the host thread.
///
/// Carries just enough of the pile for the network thread to serve
/// peer requests: per-hash blob fetch, legacy pin-root scope checks, and a
/// quick presence check.
pub trait AnySnapshot: Send + 'static {
    fn get_blob(&self, hash: &RawHash) -> Option<anybytes::Bytes>;
    fn has_blob(&self, hash: &RawHash) -> bool;
    fn legacy_head(&self, pin: &LocalPinId) -> Option<RawHash>;
}

impl<R> AnySnapshot for StoreSnapshot<R>
where
    R: triblespace_core::repo::BlobStoreGet
        + triblespace_core::repo::BlobStoreList
        + Send
        + 'static,
{
    fn get_blob(&self, hash: &RawHash) -> Option<anybytes::Bytes> {
        use triblespace_core::blob::encodings::UnknownBlob;
        use triblespace_core::inline::Inline;
        use triblespace_core::inline::encodings::hash::Handle;
        let handle = Inline::<Handle<UnknownBlob>>::new(*hash);
        self.reader.get::<anybytes::Bytes, UnknownBlob>(handle).ok()
    }

    fn has_blob(&self, hash: &RawHash) -> bool {
        use triblespace_core::blob::encodings::UnknownBlob;
        use triblespace_core::inline::Inline;
        use triblespace_core::inline::encodings::hash::Handle;
        let handle = Inline::<Handle<UnknownBlob>>::new(*hash);
        self.reader
            .get::<anybytes::Bytes, UnknownBlob>(handle)
            .is_ok()
    }

    fn legacy_head(&self, pin: &LocalPinId) -> Option<RawHash> {
        self.legacy_heads.get(pin).copied()
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

/// Per-fetch bound on candidate providers. Eight ordered DHT candidates bounds
/// deduplication work and worst-case dial fanout (each attempt is further
/// bounded by the caller's overall fetch budget).
const PROVIDER_FANOUT_CAP: usize = 8;

/// Transport-bound implementation of [`NetCapability`]. Holds exactly
/// what the fetch needs; built in the host once the transport exists.
struct NetCap<T: Transport> {
    transport: T,
    pool: SharedPool<T::Conn>,
    self_cap: Arc<Mutex<RawHash>>,
}

impl<T: Transport> NetCapability for NetCap<T> {
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Vec<u8>>> {
        let t = self.transport.clone();
        let pool = self.pool.clone();
        let self_cap = *self.self_cap.lock().unwrap();
        Box::pin(async move { fetch_one(&t, &hash, &pool, &self_cap).await })
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

    /// Activate a newly pinned outbound authentication credential. This is a
    /// command rather than shared-store observation so callers can order it
    /// strictly after durable pin success.
    pub fn update_self_cap(&self, sig_handle: RawHash) {
        let _ = self.cmd_tx.send(NetCommand::UpdateSelfCap(sig_handle));
    }

    /// Dispatch a freshly-signed (cap, sig) blob pair to `subject`.
    /// Fire-and-forget — the network thread handles the dial,
    /// `OP_DELIVER_CAP`, and connection teardown. Used by asserted-policy
    /// credential redispatch.
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

    /// Fail closed when a post-mutation serving snapshot cannot be built.
    /// Keeping the previous snapshot would preserve obsolete authorization
    /// roots after a deletion or reclassification.
    pub fn clear_snapshot(&self) {
        *self.snapshot.lock().unwrap() = None;
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

/// Deadline for a single `OP_GET_BLOB` request + full response on an
/// established connection. On expiry
/// the op reports an error and the caller's existing
/// evict-and-try-next-provider path takes over. Total-op rather than
/// progress-based: each response is bounded by the protocol's explicit
/// transport envelope; revisit with idle-deadlines when blob transfer becomes
/// chunked or streaming.
const OP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Connect to a peer over the pile-sync ALPN and immediately present
/// our capability so subsequent ops are authorised. Protocol v5 makes
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
    } = harness;

    let my_id: PeerId = transport.local_id();
    let self_cap = Arc::new(Mutex::new(config.self_cap));

    // Host-wide singleflight connection pool — one authed
    // connection per remote peer, reused across lazy blob fetches and exact
    // capability-blob loads. See `SharedPool` docs for the OnceCell-based dial
    // deduplication.
    let conn_pool: SharedPool<T::Conn> = new_shared_pool();

    // Publish the inline-fetch capability now that the transport exists.
    // `Peer::fetch_blob` parks on this slot until it's filled, which is
    // how the inline read path handles the construction-ordering the old
    // `FetchBlob` command channel used to buffer past.
    let _ = cap_tx.send(Some(Arc::new(NetCap {
        transport: transport.clone(),
        pool: conn_pool.clone(),
        self_cap: self_cap.clone(),
    }) as Arc<dyn NetCapability>));
    // One total envelope covers every unauthenticated connection. Non-proof
    // work has a limit one smaller, reserving progress for an exact proof
    // callback when cold AUTH or DELIVER verification occupies the other
    // slots.
    let preauth_slots = Arc::new(tokio::sync::Semaphore::new(PREAUTH_TOTAL_LIMIT));
    let nonproof_slots = Arc::new(tokio::sync::Semaphore::new(PREAUTH_NONPROOF_LIMIT));
    let authenticated_connection_slots =
        Arc::new(tokio::sync::Semaphore::new(AUTHENTICATED_CONNECTION_LIMIT));
    let postauth_stream_slots = Arc::new(tokio::sync::Semaphore::new(POSTAUTH_STREAM_LIMIT));
    let inbound_subjects = InboundSubjectRegistry::default();
    let cap_request_slots = Arc::new(tokio::sync::Semaphore::new(CAP_REQUEST_QUEUE_LIMIT));
    let cap_delivery_slots = Arc::new(tokio::sync::Semaphore::new(CAP_DELIVERY_QUEUE_LIMIT));
    let cap_confirmation_slots =
        Arc::new(tokio::sync::Semaphore::new(CAP_CONFIRMATION_QUEUE_LIMIT));

    // Our own pubkey — the expected `cap_subject` of any cap
    // delivered to us via OP_DELIVER_CAP.
    let our_pubkey = ed25519_dalek::VerifyingKey::from_bytes(&my_id)
        .expect("transport local id is an ed25519 pubkey");

    // ── Inbound connections: dispatch by ALPN to the protocol handlers.
    // Pile-sync retains an authenticated multiplexed connection; the open
    // auth-handshake ALPN is deliberately one operation per connection.
    let snapshot_handler = SnapshotHandler {
        snapshot: snapshot.clone(),
        team_root: config.team_root,
        transport: transport.clone(),
        events: events.clone(),
        confirmation_slots: cap_confirmation_slots,
        authenticated_connection_slots,
        postauth_stream_slots,
    };
    let handshake_handler = HandshakeHandler {
        events: events.clone(),
        team_root: config.team_root,
        our_pubkey,
        snapshot: snapshot.clone(),
        transport: transport.clone(),
        request_slots: cap_request_slots,
        delivery_slots: cap_delivery_slots,
        nonproof_slots: nonproof_slots.clone(),
    };
    let mut incoming = incoming;
    tokio::spawn(async move {
        while let Some(inc) = incoming.recv().await {
            if inc.alpn != PILE_SYNC_ALPN && inc.alpn != crate::handshake::AUTH_HANDSHAKE_ALPN {
                debug!(alpn = %String::from_utf8_lossy(inc.alpn), "incoming conn on unknown alpn; dropping");
                continue;
            }

            // The transport has already authenticated `remote_id` as the TLS
            // subject. Reserve that subject before doing any application-auth
            // work or spawning a handler: one peer gets one multiplexed
            // pile-sync connection, and duplicates fail without becoming
            // hidden waiter tasks. The RAII lease is handed to the connection
            // handler and releases the subject on every exit path.
            let subject_lease = if inc.alpn == PILE_SYNC_ALPN {
                match inbound_subjects.try_acquire(inc.conn.remote_id()) {
                    Some(lease) => Some(lease),
                    None => {
                        debug!(
                            peer = %hex::encode(&inc.conn.remote_id()[..4]),
                            "inbound pile-sync connection already live for TLS subject; rejecting"
                        );
                        inc.conn.close(0, b"duplicate authenticated subject");
                        continue;
                    }
                }
            } else {
                None
            };

            // Admission happens before spawning. An unauthenticated peer may
            // hold a stream open forever, so awaiting a permit (or spawning a
            // task which awaits one) would merely move the unbounded queue.
            let Ok(preauth_permit) = preauth_slots.clone().try_acquire_owned() else {
                debug!(
                    alpn = %String::from_utf8_lossy(inc.alpn),
                    "pre-authentication connection limit reached; rejecting"
                );
                inc.conn.close(0, b"pre-authentication capacity");
                continue;
            };

            if inc.alpn == PILE_SYNC_ALPN {
                let Ok(nonproof_permit) = nonproof_slots.clone().try_acquire_owned() else {
                    debug!("non-proof pre-authentication capacity reached; rejecting pile-sync");
                    inc.conn.close(0, b"pre-authentication capacity");
                    continue;
                };
                let h = snapshot_handler.clone();
                let subject_lease =
                    subject_lease.expect("pile-sync connection acquired a subject lease");
                tokio::spawn(async move {
                    h.handle(inc.conn, preauth_permit, nonproof_permit, subject_lease)
                        .await
                });
            } else {
                let h = handshake_handler.clone();
                let close = inc.conn.clone();
                tokio::spawn(async move {
                    if tokio::time::timeout(
                        CAP_CHAIN_LOAD_DEADLINE,
                        h.handle(inc.conn, preauth_permit),
                    )
                    .await
                    .is_err()
                    {
                        debug!("auth-handshake pre-authentication deadline exceeded");
                        close.close(0, b"pre-authentication deadline");
                    }
                });
            }
        }
    });

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
                NetCommand::UpdateSelfCap(successor) => {
                    let predecessor = {
                        let mut active = self_cap.lock().unwrap();
                        std::mem::replace(&mut *active, successor)
                    };
                    if predecessor != successor {
                        debug!(
                            predecessor = %hex::encode(&predecessor[..4]),
                            successor = %hex::encode(&successor[..4]),
                            "outbound authentication credential rotated; evicting pool"
                        );
                        pool_clear(&conn_pool, b"authentication credential rotated").await;
                    }
                }
                NetCommand::DeliverCap {
                    subject,
                    cap_bytes,
                    sig_bytes,
                } => {
                    // Open a fresh connection on the auth-handshake
                    // ALPN, send OP_DELIVER_CAP, close. On STATUS_OK
                    // ack only says the bytes reached the recipient. Positive
                    // evidence arrives later from OP_AUTH as
                    // `NetEvent::CapDeliveryConfirmed`; the Peer binds that
                    // exact presented signature to a current asserted grant.
                    // On any delivery failure (connect/send/non-OK), no
                    // confirmation event exists and issuer policy remains
                    // unauthenticated.
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

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Resolve distinct DHT providers, excluding this node and one caller-supplied
/// peer that was already attempted. The lookup is deadline-bounded.
async fn dht_providers_except<T: Transport>(
    t: &T,
    hash: &RawHash,
    excluded: PeerId,
) -> Vec<PeerId> {
    let my_id = t.local_id();
    trace!(hash = %hex::encode(&hash[..4]), "DHT provider lookup awaiting");
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
    trace!(hash = %hex::encode(&hash[..4]), n = discovered.len(), "DHT provider lookup returned");
    dht_provider_candidates(my_id, excluded, discovered)
}

fn dht_provider_candidates(
    my_id: PeerId,
    excluded: PeerId,
    discovered: impl IntoIterator<Item = PeerId>,
) -> Vec<PeerId> {
    let mut providers = Vec::new();
    let mut seen = HashSet::new();
    for provider in discovered {
        if provider != my_id && provider != excluded && seen.insert(provider) {
            providers.push(provider);
            if providers.len() == PROVIDER_FANOUT_CAP {
                break;
            }
        }
    }
    providers
}

/// Host-wide connection pool: one authed connection per remote peer, shared
/// across lazy blob fetches and exact capability-chain loads.
///
/// `OnceCell` per peer provides automatic singleflight: the first
/// task to encounter a missing entry runs the dial; concurrent tasks
/// await the same `OnceCell` and reuse the resulting connection. No
/// dial storm when several lazy reads target the same peer concurrently.
///
/// iroh QUIC multiplexes streams cheaply on a single connection; our
/// `serve_stream` reuses first-stream auth state for later bi-streams and a
/// host-wide semaphore bounds their concurrent execution. So one connection
/// per peer is enough.
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

/// Remove and close every initialized pooled connection. Uninitialized
/// singleflight cells are removed as well: a dial already in flight may still
/// complete for its current caller, but it cannot become reachable from the
/// pool after a credential rotation.
async fn pool_clear<C: Conn>(pool: &SharedPool<C>, reason: &[u8]) {
    let removed = {
        let mut guard = pool.lock().await;
        std::mem::take(&mut *guard)
    };
    for cell in removed.into_values() {
        if let Some(conn) = cell.get() {
            conn.close(0, reason);
        }
    }
}

/// Fetch a single blob via distinct DHT providers. Returns the first
/// content-verified response.
async fn fetch_one<T: Transport>(
    t: &T,
    hash: &RawHash,
    pool: &SharedPool<T::Conn>,
    self_cap: &RawHash,
) -> Option<Vec<u8>> {
    let providers = dht_providers_except(t, hash, t.local_id()).await;
    fetch_from_providers(t, hash, pool, &providers, self_cap).await
}

/// Try `providers` in order for a single blob: pooled authed connection,
/// OP_GET_BLOB with the per-op deadline, evict-and-try-next on
/// connection errors or hash mismatches. First content-verified success wins.
/// The provider-iteration tail of [`fetch_one`], split out for exact provider
/// attempts and testability.
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

/// Fetch one exact member of a capability proof over the narrowly open
/// auth-handshake ALPN. The serving peer verifies the complete chain locally
/// and serves only a handle touched by that verification, so this path does
/// not inherit ordinary branch-data scope and is not a generic blob oracle.
async fn fetch_capability_blob_one<T: Transport>(
    t: &T,
    provider: PeerId,
    leaf_sig: &RawHash,
    subject: &PeerId,
    requested: &RawHash,
) -> Option<Vec<u8>> {
    if provider == t.local_id() {
        return None;
    }
    let conn = match tokio::time::timeout(
        DIAL_DEADLINE,
        t.dial(provider, crate::handshake::AUTH_HANDSHAKE_ALPN),
    )
    .await
    {
        Ok(Ok(conn)) => conn,
        Ok(Err(error)) => {
            debug!(error = %error, provider = %hex::encode(&provider[..4]), "capability proof provider dial failed");
            return None;
        }
        Err(_) => {
            debug!(provider = %hex::encode(&provider[..4]), "capability proof provider dial exceeded deadline");
            return None;
        }
    };
    let response = tokio::time::timeout(
        OP_DEADLINE,
        crate::handshake::fetch_capability_blob(&conn, leaf_sig, subject, requested),
    )
    .await;
    conn.close(0, b"capability proof fetch complete");
    match response {
        Ok(Ok(Some(bytes))) if blake3::hash(&bytes).as_bytes() == requested => Some(bytes),
        Ok(Ok(Some(_))) => {
            warn!(provider = %hex::encode(&provider[..4]), requested = %hex::encode(&requested[..4]), "capability proof provider returned wrong content");
            None
        }
        Ok(Ok(None)) => None,
        Ok(Err(error)) => {
            debug!(error = %error, provider = %hex::encode(&provider[..4]), "capability proof fetch failed");
            None
        }
        Err(_) => {
            debug!(provider = %hex::encode(&provider[..4]), "capability proof fetch exceeded deadline");
            None
        }
    }
}

async fn fetch_capability_blob_exact<T: Transport>(
    t: &T,
    publisher: PeerId,
    leaf_sig: &RawHash,
    subject: &PeerId,
    requested: &RawHash,
) -> Option<Vec<u8>> {
    if let Some(bytes) = fetch_capability_blob_one(t, publisher, leaf_sig, subject, requested).await
    {
        return Some(bytes);
    }
    for provider in dht_providers_except(t, requested, publisher).await {
        if let Some(bytes) =
            fetch_capability_blob_one(t, provider, leaf_sig, subject, requested).await
        {
            return Some(bytes);
        }
    }
    None
}

/// Capability blobs are deliberately tiny compared with ordinary content.
/// Pre-authentication chain loading gets its own envelope rather than reusing
/// the 256 MiB generic blob transport or the conservative unknown-blob walker.
const CAP_CHAIN_BLOB_LIMIT: usize = crate::handshake::MAX_BLOB_BYTES as usize;
const CAP_CHAIN_BLOB_COUNT_LIMIT: usize = triblespace_core::repo::capability::MAX_CHAIN_DEPTH + 1;
const CAP_CHAIN_TOTAL_BYTES: usize = CAP_CHAIN_BLOB_LIMIT * CAP_CHAIN_BLOB_COUNT_LIMIT;
const CAP_CHAIN_LOAD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
const PREAUTH_TOTAL_LIMIT: usize = 8;
const PREAUTH_NONPROOF_LIMIT: usize = PREAUTH_TOTAL_LIMIT - 1;
/// Maximum live inbound pile-sync connections after successful authentication.
/// Outbound callers already pool one connection per peer; accepting more than
/// this many inbound connections would only retain idle transport and handler
/// state. Saturation is fail-fast rather than a semaphore wait queue.
const AUTHENTICATED_CONNECTION_LIMIT: usize = 64;
/// Maximum post-authentication pile-sync operations executing across all
/// inbound connections. Every admitted operation also has [`OP_DEADLINE`], so
/// a peer cannot retain a slot indefinitely by stalling its stream.
const POSTAUTH_STREAM_LIMIT: usize = 64;
/// Maximum post-authentication operations executing on one subject's sole
/// inbound pile-sync connection. This local envelope is nested beneath the
/// host-wide [`POSTAUTH_STREAM_LIMIT`], preventing one authenticated subject
/// from occupying the entire host.
const POSTAUTH_STREAM_PER_CONNECTION_LIMIT: usize = 8;
/// Maximum parsed join requests waiting for the synchronous Peer/policy loop.
/// The permit travels inside `NetEvent::CapRequest`, so capacity is released
/// exactly when the queued request is consumed or dropped.
const CAP_REQUEST_QUEUE_LIMIT: usize = 128;
/// Maximum fully verified capability bundles waiting for synchronous
/// persistence and team-cap pinning.
const CAP_DELIVERY_QUEUE_LIMIT: usize = 32;
const CAP_CONFIRMATION_QUEUE_LIMIT: usize = 128;

/// Host-local set of TLS subjects that currently own an inbound pile-sync
/// connection. The transport authenticates `remote_id`; application-level
/// capability auth then determines what that subject may do on its one
/// multiplexed connection.
#[derive(Clone, Default)]
struct InboundSubjectRegistry {
    live: Arc<Mutex<HashSet<PeerId>>>,
}

impl InboundSubjectRegistry {
    /// Acquire a subject without waiting. The returned lease removes the
    /// subject on drop, including authentication failure, deadline, transport
    /// closure, and task cancellation paths.
    fn try_acquire(&self, subject: PeerId) -> Option<InboundSubjectLease> {
        let mut live = self.live.lock().unwrap();
        if !live.insert(subject) {
            return None;
        }
        Some(InboundSubjectLease {
            registry: self.clone(),
            subject,
        })
    }
}

struct InboundSubjectLease {
    registry: InboundSubjectRegistry,
    subject: PeerId,
}

impl Drop for InboundSubjectLease {
    fn drop(&mut self) {
        self.registry.live.lock().unwrap().remove(&self.subject);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostauthAdmissionError {
    Connection,
    Global,
}

/// The two nested permits for one executing post-authentication stream.
/// Acquiring is entirely non-blocking; partial acquisition rolls back by RAII.
struct PostauthStreamPermit {
    _connection: tokio::sync::OwnedSemaphorePermit,
    _global: tokio::sync::OwnedSemaphorePermit,
}

fn try_acquire_postauth_stream(
    connection_slots: &Arc<tokio::sync::Semaphore>,
    global_slots: &Arc<tokio::sync::Semaphore>,
) -> Result<PostauthStreamPermit, PostauthAdmissionError> {
    let connection = connection_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| PostauthAdmissionError::Connection)?;
    let global = global_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| PostauthAdmissionError::Global)?;
    Ok(PostauthStreamPermit {
        _connection: connection,
        _global: global,
    })
}

struct LoadedCapabilityChain {
    /// Provided, local, and newly fetched blobs used by `verify_chain`.
    blobs: std::collections::BTreeMap<RawHash, anybytes::Bytes>,
    bytes: usize,
}

impl LoadedCapabilityChain {
    fn new(provided: std::collections::BTreeMap<RawHash, anybytes::Bytes>) -> anyhow::Result<Self> {
        let mut loaded = Self {
            blobs: std::collections::BTreeMap::new(),
            bytes: 0,
        };
        for (hash, bytes) in provided {
            loaded.insert(hash, bytes)?;
        }
        Ok(loaded)
    }

    fn insert(&mut self, hash: RawHash, bytes: anybytes::Bytes) -> anyhow::Result<()> {
        if self.blobs.contains_key(&hash) {
            return Ok(());
        }
        if *blake3::hash(&bytes).as_bytes() != hash {
            return Err(anyhow::anyhow!(
                "capability blob failed content verification"
            ));
        }
        if bytes.len() > CAP_CHAIN_BLOB_LIMIT {
            return Err(anyhow::anyhow!(
                "capability blob has {} bytes, exceeds limit {}",
                bytes.len(),
                CAP_CHAIN_BLOB_LIMIT
            ));
        }
        if self.blobs.len() >= CAP_CHAIN_BLOB_COUNT_LIMIT {
            return Err(anyhow::anyhow!(
                "capability chain exceeds {} blobs",
                CAP_CHAIN_BLOB_COUNT_LIMIT
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= CAP_CHAIN_TOTAL_BYTES)
            .ok_or_else(|| anyhow::anyhow!("capability chain byte budget exhausted"))?;
        self.blobs.insert(hash, bytes);
        Ok(())
    }
}

async fn load_capability_blob<T: Transport>(
    t: &T,
    publisher: PeerId,
    leaf_sig: RawHash,
    subject: PeerId,
    hash: RawHash,
    snapshot: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    loaded: &mut LoadedCapabilityChain,
) -> anyhow::Result<anybytes::Bytes> {
    if let Some(bytes) = loaded.blobs.get(&hash) {
        return Ok(bytes.clone());
    }
    if loaded.blobs.len() >= CAP_CHAIN_BLOB_COUNT_LIMIT {
        return Err(anyhow::anyhow!(
            "capability chain exceeds {} blobs",
            CAP_CHAIN_BLOB_COUNT_LIMIT
        ));
    }
    if let Some(bytes) = snapshot
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|snapshot| snapshot.get_blob(&hash))
    {
        loaded.insert(hash, bytes.clone())?;
        return Ok(bytes);
    }
    let bytes = fetch_capability_blob_exact(t, publisher, &leaf_sig, &subject, &hash)
        .await
        .ok_or_else(|| anyhow::anyhow!("capability blob unavailable: {}", hex::encode(hash)))?;
    let bytes = anybytes::Bytes::from_source(bytes);
    loaded.insert(hash, bytes.clone())?;
    Ok(bytes)
}

/// Verify a capability while loading only the exact handle requested by the
/// verifier at each step. A fetched parent is not allowed to name the next
/// network request until its prefix of the chain has passed all signature,
/// subject, expiry, delegation, and scope checks. Generic aligned-word child
/// hints are therefore never part of pre-authentication work.
#[allow(clippy::too_many_arguments)]
async fn verify_capability_chain_exact<T: Transport>(
    t: &T,
    publisher: PeerId,
    leaf_sig: RawHash,
    expected_subject: ed25519_dalek::VerifyingKey,
    team_root: ed25519_dalek::VerifyingKey,
    snapshot: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    provided: std::collections::BTreeMap<RawHash, anybytes::Bytes>,
) -> anyhow::Result<(
    triblespace_core::repo::capability::VerifiedCapability,
    std::collections::BTreeMap<RawHash, anybytes::Bytes>,
)> {
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::inline::Inline;
    use triblespace_core::inline::encodings::hash::Handle;

    let mut loaded = LoadedCapabilityChain::new(provided)?;
    let leaf_sig_handle: Inline<Handle<SimpleArchive>> = Inline::new(leaf_sig);
    let mut attempted = std::collections::BTreeSet::new();

    loop {
        let result = triblespace_core::repo::capability::verify_chain(
            team_root,
            leaf_sig_handle,
            expected_subject,
            |handle| {
                loaded
                    .blobs
                    .get(&handle.raw)
                    .cloned()
                    .map(Blob::<SimpleArchive>::new)
            },
        );
        match result {
            Ok(verified) => return Ok((verified, loaded.blobs)),
            Err(triblespace_core::repo::capability::VerifyError::MissingBlob(handle)) => {
                if !attempted.insert(handle.raw) {
                    return Err(anyhow::anyhow!(
                        "capability verifier repeatedly requested unresolved blob {}",
                        hex::encode(handle.raw)
                    ));
                }
                load_capability_blob(
                    t,
                    publisher,
                    leaf_sig,
                    expected_subject.to_bytes(),
                    handle.raw,
                    snapshot,
                    &mut loaded,
                )
                .await?;
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "capability chain verification failed: {error:?}"
                ));
            }
        }
    }
}

// ── Protocol handler ─────────────────────────────────────────────────

#[derive(Clone)]
struct SnapshotHandler<T: Transport> {
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Verifies all incoming capability chains. Required — protocol v5
    /// has mandatory auth.
    team_root: ed25519_dalek::VerifyingKey,
    /// Transport for exact outbound capability-blob loads when OP_AUTH
    /// references a handle absent from the local snapshot.
    transport: T,
    /// Channel back to the Peer for delivery-confirmation control events.
    /// Cold OP_AUTH proof members remain connection-local so arbitrary valid
    /// remote credentials cannot grow durable local state.
    events: mpsc::Sender<NetEvent>,
    /// Bounded best-effort delivery-confirmation event queue.
    confirmation_slots: Arc<tokio::sync::Semaphore>,
    /// Held for the full lifetime of each successfully authenticated inbound
    /// pile-sync connection.
    authenticated_connection_slots: Arc<tokio::sync::Semaphore>,
    /// Held for the full lifetime of each executing post-authentication
    /// stream, including its operation deadline.
    postauth_stream_slots: Arc<tokio::sync::Semaphore>,
}

/// Protocol handler for `/triblespace/auth-handshake/2`. Accepts
/// incoming `OP_REQUEST_CAP` and `OP_DELIVER_CAP` streams and
/// forwards their payloads to the Peer's event channel. Request ACKs wait for
/// the Peer's durable completion receipt; all policy (approve / queue / reject;
/// verify / pin / drop) still lives in the receiving Peer.
#[derive(Clone)]
struct HandshakeHandler<T: Transport> {
    events: mpsc::Sender<NetEvent>,
    /// Permits held by queued `CapRequest` events. Admission is non-blocking:
    /// an unauthenticated sender cannot create a hidden waiter task when the
    /// policy loop is behind.
    request_slots: Arc<tokio::sync::Semaphore>,
    /// Permits held by complete verified delivery bundles until the Peer has
    /// consumed them.
    delivery_slots: Arc<tokio::sync::Semaphore>,
    /// Shared admission for every pre-authentication operation except the
    /// exact proof-fetch callback, which retains one reserved total slot.
    nonproof_slots: Arc<tokio::sync::Semaphore>,
    /// Team root pubkey — verifies the delivered cap's chain at
    /// `OP_DELIVER_CAP` time so STATUS_OK means "we'd accept this".
    team_root: ed25519_dalek::VerifyingKey,
    /// Our own pubkey — the expected `cap_subject` of any cap
    /// delivered to us.
    our_pubkey: ed25519_dalek::VerifyingKey,
    /// Snapshot for local-pile blob lookup during verify.
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Transport + pool load only an exact handle returned by
    /// `VerifyError::MissingBlob`, trying the dialer before DHT providers.
    /// The credential is the just-delivered sig handle itself (see the
    /// OP_DELIVER_CAP arm), so no separate self_cap is needed here.
    transport: T,
}

/// Decode only the identity-bearing part of an incoming partial capability.
/// This gate runs before the request is admitted to the Peer queue, so the
/// unauthenticated handshake cannot spend durable policy capacity on behalf of
/// some other subject. Full shape and policy validation still belongs to the
/// synchronous policy layer.
fn partial_cap_names_subject(
    partial_cap_bytes: &anybytes::Bytes,
    expected_subject: &PublisherKey,
) -> bool {
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::blob::{Blob, TryFromBlob};
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::trible::TribleSet;

    let Ok(cap): Result<TribleSet, _> =
        TribleSet::try_from_blob(Blob::<SimpleArchive>::new(partial_cap_bytes.clone()))
    else {
        return false;
    };
    let mut subjects = find!(
        (
            cap_entity: triblespace_core::id::Id,
            subject: ed25519_dalek::VerifyingKey,
        ),
        pattern!(&cap, [{
            ?cap_entity @ triblespace_core::repo::capability::cap_subject: ?subject,
        }])
    );
    matches!(
        (subjects.next(), subjects.next()),
        (Some((_entity, subject)), None) if subject.to_bytes() == *expected_subject
    )
}

/// Admit one parsed request to the synchronous policy loop and wait for its
/// durability receipt. `false` is reserved for an explicit policy refusal.
/// If the policy loop drops the completion sender after a storage failure, the
/// append outcome may be ambiguous, so the wire reports indeterminate and an
/// exact idempotent replay can resolve it. The surrounding handshake deadline
/// independently bounds this wait.
async fn enqueue_cap_request(
    events: &mpsc::Sender<NetEvent>,
    request_slots: &Arc<tokio::sync::Semaphore>,
    requester: PublisherKey,
    partial_cap_bytes: anybytes::Bytes,
) -> u8 {
    let Ok(admission) = request_slots.clone().try_acquire_owned() else {
        debug!(
            limit = CAP_REQUEST_QUEUE_LIMIT,
            "OP_REQUEST_CAP policy queue is full"
        );
        return crate::handshake::STATUS_REJECTED;
    };
    let (completion, durable) = tokio::sync::oneshot::channel();
    if events
        .send(NetEvent::CapRequest {
            requester,
            partial_cap_bytes,
            admission,
            completion,
        })
        .is_err()
    {
        return crate::handshake::STATUS_REJECTED;
    }
    match durable.await {
        Ok(true) => crate::handshake::STATUS_OK,
        Ok(false) => crate::handshake::STATUS_REJECTED,
        Err(_) => crate::handshake::STATUS_INDETERMINATE,
    }
}

impl<T: Transport> HandshakeHandler<T> {
    async fn handle(
        &self,
        connection: T::Conn,
        _preauth_permit: tokio::sync::OwnedSemaphorePermit,
    ) {
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
        let request_slots = self.request_slots.clone();
        let delivery_slots = self.delivery_slots.clone();
        let nonproof_slots = self.nonproof_slots.clone();
        let span = info_span!(
            "auth-handshake",
            peer = %hex::encode(&peer_pubkey_bytes[..4]),
        );
        async move {
            // This ALPN is deliberately one operation per connection. It has
            // no authenticated state to amortize, so accepting an unbounded
            // stream sequence would let one admitted peer retain a scarce
            // pre-authentication slot indefinitely.
            loop {
                let Some((mut send, mut recv)) = connection.accept_bi().await else {
                    debug!("accept_bi ended; handshake connection closing");
                    break;
                };
                match crate::handshake::read_incoming(&mut recv).await {
                    Ok(Some(crate::handshake::IncomingOp::Request {
                        partial_cap_bytes,
                    })) => {
                        let Ok(_nonproof_permit) =
                            nonproof_slots.clone().try_acquire_owned()
                        else {
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_REJECTED,
                            )
                            .await;
                            break;
                        };
                        let status = if !partial_cap_names_subject(
                            &partial_cap_bytes,
                            &peer_pubkey_bytes,
                        ) {
                            debug!("OP_REQUEST_CAP subject is malformed or differs from TLS peer");
                            crate::handshake::STATUS_REJECTED
                        } else {
                            enqueue_cap_request(
                                &events,
                                &request_slots,
                                peer_pubkey_bytes,
                                partial_cap_bytes,
                            )
                            .await
                        };
                        let _ = crate::handshake::respond(&mut send, status).await;
                    }
                    Ok(Some(crate::handshake::IncomingOp::FetchCapabilityBlob {
                        leaf_sig,
                        subject,
                        requested,
                    })) => {
                        use triblespace_core::blob::Blob;
                        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
                        use triblespace_core::inline::Inline;
                        use triblespace_core::inline::encodings::hash::Handle;

                        // This endpoint is open but narrow: prove locally that
                        // the complete chain is valid, and retain only bounded,
                        // content-correct handles actually returned to the
                        // verifier. The caller learns no arbitrary store blob.
                        let payload = ed25519_dalek::VerifyingKey::from_bytes(&subject)
                            .ok()
                            .and_then(|subject| {
                                let guard = snapshot.lock().unwrap();
                                let snap = guard.as_ref()?;
                                let mut proof_blobs = std::collections::BTreeMap::new();
                                let result = triblespace_core::repo::capability::verify_chain(
                                    team_root,
                                    Inline::<Handle<SimpleArchive>>::new(leaf_sig),
                                    subject,
                                    |handle| {
                                        let bytes = snap.get_blob(&handle.raw)?;
                                        if bytes.len()
                                            > crate::handshake::MAX_BLOB_BYTES as usize
                                            || blake3::hash(&bytes).as_bytes() != &handle.raw
                                        {
                                            return None;
                                        }
                                        proof_blobs.insert(handle.raw, bytes.clone());
                                        Some(Blob::new(bytes))
                                    },
                                );
                                result.ok()?;
                                proof_blobs.remove(&requested)
                            });
                        if payload.is_none() {
                            debug!(
                                leaf_sig = %hex::encode(&leaf_sig[..4]),
                                requested = %hex::encode(&requested[..4]),
                                "capability proof fetch rejected"
                            );
                        }
                        let _ = crate::handshake::respond_capability_blob(
                            &mut send,
                            payload.as_deref(),
                        )
                        .await;
                    }
                    Ok(Some(crate::handshake::IncomingOp::Deliver {
                        cap_bytes,
                        sig_bytes,
                    })) => {
                        let Ok(_nonproof_permit) =
                            nonproof_slots.clone().try_acquire_owned()
                        else {
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_REJECTED,
                            )
                            .await;
                            break;
                        };
                        use triblespace_core::blob::{Blob, TryFromBlob};
                        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
                        use triblespace_core::inline::Inline;
                        use triblespace_core::inline::encodings::hash::Handle;
                        use triblespace_core::trible::TribleSet;
                        use triblespace_core::macros::{find, pattern};

                        let cap_hash: RawHash = *blake3::hash(&cap_bytes).as_bytes();
                        let sig_hash: RawHash = *blake3::hash(&sig_bytes).as_bytes();

                        // Cheap, entirely in-band gates before any exact chain
                        // load. The signature must uniquely name the supplied
                        // cap, and that cap's unique declared issuer must be the
                        // TLS-authenticated dialer. Without both checks an open
                        // handshake peer could make the verifier request an
                        // attacker-chosen leaf-cap handle before any signature
                        // had been verified.
                        let Ok(cap_set): Result<TribleSet, _> = TribleSet::try_from_blob(
                            Blob::<SimpleArchive>::new(cap_bytes.clone()),
                        ) else {
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_MALFORMED,
                            )
                            .await;
                            break;
                        };
                        let Ok(sig_set): Result<TribleSet, _> = TribleSet::try_from_blob(
                            Blob::<SimpleArchive>::new(sig_bytes.clone()),
                        ) else {
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_MALFORMED,
                            )
                            .await;
                            break;
                        };
                        let mut signed_caps = find!(
                            (
                                sig_entity: triblespace_core::id::Id,
                                signed_cap: Inline<Handle<SimpleArchive>>,
                            ),
                            pattern!(&sig_set, [{
                                ?sig_entity @ triblespace_core::repo::capability::sig_signs: ?signed_cap,
                            }])
                        );
                        let signed_cap = match (signed_caps.next(), signed_caps.next()) {
                            (Some((_entity, handle)), None) => handle,
                            _ => {
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_MALFORMED,
                                )
                                .await;
                                break;
                            }
                        };
                        if signed_cap.raw != cap_hash {
                            warn!(
                                supplied_cap = %hex::encode(&cap_hash[..4]),
                                signed_cap = %hex::encode(&signed_cap.raw[..4]),
                                "OP_DELIVER_CAP: in-band signature names a different cap"
                            );
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_MALFORMED,
                            )
                            .await;
                            break;
                        }

                        let mut issuers = find!(
                            (
                                cap_entity: triblespace_core::id::Id,
                                issuer: ed25519_dalek::VerifyingKey,
                            ),
                            pattern!(&cap_set, [{
                                ?cap_entity @ triblespace_core::repo::capability::cap_issuer: ?issuer,
                            }])
                        );
                        let declared_issuer = match (issuers.next(), issuers.next()) {
                            (Some((_entity, issuer)), None) => Some(issuer),
                            _ => None,
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
                                break;
                            }
                            None => {
                                warn!("OP_DELIVER_CAP: cap blob malformed or missing cap_issuer; rejecting");
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    crate::handshake::STATUS_MALFORMED,
                                )
                                .await;
                                break;
                            }
                        }

                        let Ok(delivery_admission) =
                            delivery_slots.clone().try_acquire_owned()
                        else {
                            debug!(
                                limit = CAP_DELIVERY_QUEUE_LIMIT,
                                "OP_DELIVER_CAP persistence queue is full"
                            );
                            let _ = crate::handshake::respond(
                                &mut send,
                                crate::handshake::STATUS_REJECTED,
                            )
                            .await;
                            break;
                        };

                        // The leaf pair arrived in-band. Re-run the verifier
                        // after loading only the exact missing parent handle it
                        // reports; each already-loaded prefix must verify before
                        // it can authorize another network request. The narrow
                        // proof-fetch endpoint serves only members of this same
                        // fully valid team-root chain.
                        let mut provided = std::collections::BTreeMap::new();
                        provided.insert(cap_hash, cap_bytes.clone());
                        provided.insert(sig_hash, sig_bytes.clone());
                        let result = verify_capability_chain_exact(
                            &transport,
                            peer_pubkey_bytes,
                            sig_hash,
                            our_pubkey,
                            team_root,
                            &snapshot,
                            provided,
                        )
                        .await;

                        match result {
                            Ok((verified, mut closure)) => {
                                debug!(
                                    sig = %hex::encode(&sig_hash[..4]),
                                    issuer = %hex::encode(&peer_pubkey_bytes[..4]),
                                    "OP_DELIVER_CAP: chain verified; absorbing",
                                );
                                // The leaf pair already has dedicated fields;
                                // everything else touched by verification is
                                // retained, regardless of whether it came from
                                // the local snapshot or the network. A later
                                // snapshot/compaction boundary therefore
                                // cannot strand the newly active leaf.
                                closure.remove(&cap_hash);
                                closure.remove(&sig_hash);
                                let accepted = events.send(NetEvent::CapDelivered {
                                    issuer: peer_pubkey_bytes,
                                    cap_bytes,
                                    sig_bytes,
                                    proof_blobs: closure.into_values().collect(),
                                    authority_expires_at: verified.expires_at(),
                                    admission: delivery_admission,
                                }).is_ok();
                                let _ = crate::handshake::respond(
                                    &mut send,
                                    if accepted {
                                        crate::handshake::STATUS_OK
                                    } else {
                                        crate::handshake::STATUS_REJECTED
                                    },
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
                break;
            }
            connection.close(0, b"handshake complete");
        }
        .instrument(span)
        .await;
    }
}

impl<T: Transport> SnapshotHandler<T> {
    async fn handle(
        &self,
        connection: T::Conn,
        preauth_permit: tokio::sync::OwnedSemaphorePermit,
        nonproof_permit: tokio::sync::OwnedSemaphorePermit,
        subject_lease: InboundSubjectLease,
    ) {
        let snap = self.snapshot.clone();
        let team_root = self.team_root;
        let transport = self.transport.clone();
        let events = self.events.clone();
        let confirmation_slots = self.confirmation_slots.clone();
        let authenticated_connection_slots = self.authenticated_connection_slots.clone();
        let postauth_stream_slots = self.postauth_stream_slots.clone();

        let peer_id: PeerId = connection.remote_id();
        let span = info_span!(
            "connection",
            peer = %hex::encode(&peer_id[..4]),
            alpn = %String::from_utf8_lossy(PILE_SYNC_ALPN),
        );

        async move {
            // Keep the TLS subject reserved for this handler's entire
            // lifetime. Drop is the cleanup path for every return below.
            // Stream tasks retain clones below. Closing the accept loop must
            // not release this subject while already-admitted work still owns
            // connection clones; otherwise a reconnect could overlap the old
            // subject's tail and defeat the per-subject envelope.
            let subject_lease = Arc::new(subject_lease);
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

            let first_auth = tokio::time::timeout(CAP_CHAIN_LOAD_DEADLINE, async {
                let Some((mut first_send, mut first_recv)) = connection.accept_bi().await else {
                    return Err(anyhow::anyhow!(
                        "connection closed before mandatory OP_AUTH stream"
                    ));
                };
                let result = serve_stream(
                    &snap,
                    team_root,
                    peer_pubkey,
                    auth_state.clone(),
                    &connection,
                    true,
                    &transport,
                    &events,
                    &confirmation_slots,
                    &mut first_send,
                    &mut first_recv,
                )
                .await;
                let _ = first_send.shutdown().await;
                result
            })
            .await;

            match first_auth {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!(error = %e, "first-stream authentication failed");
                }
                Err(_) => {
                    debug!("mandatory first-stream OP_AUTH exceeded deadline");
                    connection.close(0, b"authentication deadline");
                    return;
                }
            }
            if auth_state.read().await.is_none() {
                debug!("mandatory first-stream OP_AUTH did not authenticate; closing connection");
                connection.close(0, b"authentication required");
                return;
            }

            // Exchange the two pre-authentication permits for one lifetime
            // permit without ever awaiting capacity. Authentication remains
            // inside the envelope that reserves proof-callback progress; a
            // successfully authenticated peer is retained only if the global
            // live-connection pool has room.
            let Ok(authenticated_connection_permit) =
                authenticated_connection_slots.clone().try_acquire_owned()
            else {
                debug!(
                    limit = AUTHENTICATED_CONNECTION_LIMIT,
                    "authenticated connection limit reached; closing"
                );
                connection.close(0, b"authenticated connection capacity");
                return;
            };
            drop(preauth_permit);
            drop(nonproof_permit);
            // As with the subject lease, the live-connection permit belongs
            // to the handler and every admitted stream task together.
            let authenticated_connection_permit = Arc::new(authenticated_connection_permit);
            let connection_authority = auth_state
                .read()
                .await
                .clone()
                .expect("successful first-stream authentication installed authority");
            let authority_expires_at = connection_authority.expires_at();
            let authority_expiry = wait_until_authority_expires(&connection_authority);
            tokio::pin!(authority_expiry);
            let connection_stream_slots = Arc::new(tokio::sync::Semaphore::new(
                POSTAUTH_STREAM_PER_CONNECTION_LIMIT,
            ));

            loop {
                let accepted = tokio::select! {
                    biased;
                    _ = &mut authority_expiry => {
                        debug!(
                            expires_at = %authority_expires_at,
                            "idle verified capability expired; closing connection"
                        );
                        *auth_state.write().await = None;
                        connection.close(0, b"capability expired");
                        break;
                    }
                    accepted = connection.accept_bi() => accepted,
                };
                let Some((mut send, mut recv)) = accepted else {
                    debug!("accept_bi ended; connection closing");
                    break;
                };
                // Admission happens before spawning and never waits. Closing
                // the saturated connection also drops this already-accepted
                // stream, so no application-level waiter or task can collect
                // behind the semaphore.
                let postauth_stream_permit = match try_acquire_postauth_stream(
                    &connection_stream_slots,
                    &postauth_stream_slots,
                ) {
                    Ok(permit) => permit,
                    Err(PostauthAdmissionError::Connection) => {
                        debug!(
                            limit = POSTAUTH_STREAM_PER_CONNECTION_LIMIT,
                            "per-connection post-authentication stream limit reached; closing connection"
                        );
                        connection.close(0, b"subject stream capacity");
                        break;
                    }
                    Err(PostauthAdmissionError::Global) => {
                        debug!(
                            limit = POSTAUTH_STREAM_LIMIT,
                            "global post-authentication stream limit reached; closing connection"
                        );
                        connection.close(0, b"post-authentication stream capacity");
                        break;
                    }
                };
                let snap = snap.clone();
                let auth_state = auth_state.clone();
                let transport = transport.clone();
                let events = events.clone();
                let confirmation_slots = confirmation_slots.clone();
                let stream_connection = connection.clone();
                let subject_lease = subject_lease.clone();
                let authenticated_connection_permit = authenticated_connection_permit.clone();
                tokio::spawn(
                    async move {
                        let _subject_lease = subject_lease;
                        let _authenticated_connection_permit = authenticated_connection_permit;
                        let _postauth_stream_permit = postauth_stream_permit;
                        match tokio::time::timeout(
                            OP_DEADLINE,
                            serve_stream(
                                &snap,
                                team_root,
                                peer_pubkey,
                                auth_state,
                                &stream_connection,
                                false,
                                &transport,
                                &events,
                                &confirmation_slots,
                                &mut send,
                                &mut recv,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!(error = %e, "stream handler error"),
                            Err(_) => debug!(
                                deadline = ?OP_DEADLINE,
                                "post-auth stream exceeded operation deadline"
                            ),
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

/// Apply the server side of the `GET_BLOB` transport envelope before any
/// owned copy is made. [`AnySnapshot::get_blob`] returns shared `Bytes`, so an
/// oversized mmap/archive view is rejected by length alone rather than first
/// being materialized as a `Vec`.
fn bound_outbound_blob(
    data: Option<anybytes::Bytes>,
    max_bytes: usize,
) -> Result<Option<anybytes::Bytes>, usize> {
    match data {
        Some(data) if data.len() > max_bytes => Err(data.len()),
        other => Ok(other),
    }
}

/// Wait until an authority has actually passed its inclusive upper bound.
///
/// Epoch time can move independently of Tokio's monotonic timer. Sleeping in
/// bounded operation-deadline slices notices a forward wall-clock correction
/// promptly and naturally rearms after a backward correction. Simulations move
/// both clocks together.
async fn wait_until_authority_expires(
    verified: &triblespace_core::repo::capability::VerifiedCapability,
) {
    // Bind the absolute expiry to a monotonic deadline at authentication
    // time. A backward wall-clock correction must not extend already granted
    // authority; forward corrections are still noticed within one bounded
    // slice (and every operation independently checks epoch time).
    let initial_now = crate::clock::epoch_now();
    let initial_remaining_ns = (verified.expires_at() - initial_now)
        .total_nanoseconds()
        .saturating_add(1)
        .clamp(0, u64::MAX as i128) as u64;
    let monotonic_deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_nanos(initial_remaining_ns));
    loop {
        let now = crate::clock::epoch_now();
        if verified.is_expired_at(now) {
            return;
        }
        // Capability bounds are inclusive, hence the extra nanosecond. Clamp
        // each sleep to the existing operation deadline so an epoch-clock
        // correction cannot strand an idle authenticated connection.
        let remaining_ns = (verified.expires_at() - now)
            .total_nanoseconds()
            .saturating_add(1);
        let sleep_ns = remaining_ns.clamp(1, OP_DEADLINE.as_nanos() as i128) as u64;
        tokio::time::sleep(std::time::Duration::from_nanos(sleep_ns)).await;
        if monotonic_deadline.is_none_or(|deadline| tokio::time::Instant::now() >= deadline) {
            return;
        }
    }
}

/// Recheck a connection's snapshotted authority after a complete operation
/// frame and immediately before touching the serving snapshot. Expiry is
/// terminal for the connection: clear shared state first so sibling streams
/// fail closed, then close the transport. An independent connection-lifetime
/// timer handles the idle case.
async fn authority_is_live<C: Conn>(
    verified: &triblespace_core::repo::capability::VerifiedCapability,
    auth_state: &Arc<
        tokio::sync::RwLock<Option<triblespace_core::repo::capability::VerifiedCapability>>,
    >,
    connection: &C,
) -> bool {
    if !verified.is_expired() {
        return true;
    }
    debug!(
        expires_at = %verified.expires_at(),
        "verified capability expired; closing connection"
    );
    *auth_state.write().await = None;
    connection.close(0, b"capability expired");
    false
}

/// Receive the complete blob request before checking its authority boundary.
/// Keeping the await and the check together makes it difficult to
/// accidentally authorize an opcode prefix and use that stale decision after
/// the remaining frame arrives.
async fn recv_live_blob_request<C: Conn>(
    recv: &mut C::RecvHalf,
    verified: &triblespace_core::repo::capability::VerifiedCapability,
    auth_state: &Arc<
        tokio::sync::RwLock<Option<triblespace_core::repo::capability::VerifiedCapability>>,
    >,
    connection: &C,
) -> anyhow::Result<Option<[u8; 32]>> {
    let hash = recv_hash(recv).await?;
    if !authority_is_live(verified, auth_state, connection).await {
        return Ok(None);
    }
    Ok(Some(hash))
}

#[allow(clippy::too_many_arguments)]
async fn serve_stream<T: Transport>(
    snap_arc: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    team_root: ed25519_dalek::VerifyingKey,
    peer_pubkey: ed25519_dalek::VerifyingKey,
    auth_state: Arc<
        tokio::sync::RwLock<Option<triblespace_core::repo::capability::VerifiedCapability>>,
    >,
    connection: &T::Conn,
    auth_allowed: bool,
    t: &T,
    events: &mpsc::Sender<NetEvent>,
    confirmation_slots: &Arc<tokio::sync::Semaphore>,
    send: &mut <T::Conn as Conn>::SendHalf,
    recv: &mut <T::Conn as Conn>::RecvHalf,
) -> anyhow::Result<()> {
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
        // Capability blobs are orphan content rather than branch-reachable
        // state. Complete a cold local view by following only the verifier's
        // typed MissingBlob(handle). The initial leaf sig+cap pair is located
        // before its signature can be checked; every subsequent parent load is
        // reached only through the now-verified child link. Count, size,
        // concurrency, and wall-clock envelopes bound both phases.
        let result = verify_capability_chain_exact(
            t,
            peer_pubkey.to_bytes(),
            cap_handle_raw,
            peer_pubkey,
            team_root,
            snap_arc,
            std::collections::BTreeMap::new(),
        )
        .await;

        match result {
            Ok((verified, _closure)) => {
                let granted = verified.granted_branches().map(|s| s.len()).unwrap_or(0);
                let unrestricted = verified.granted_branches().is_none();
                info!(branches = granted, unrestricted = unrestricted, "auth ok");
                // Cold proof members stay connection-local. Persisting every
                // valid remote credential would let sequential self-
                // delegation grow this node's durable store without bound.
                // Tell the Peer thread that this remote authenticated with
                // `cap_handle_raw`. If exactly one current asserted grant for
                // this subject names that signature, the Peer records the
                // positive `CredentialAuthenticated` fact.
                if let Ok(admission) = confirmation_slots.clone().try_acquire_owned() {
                    let _ = events.send(NetEvent::CapDeliveryConfirmed {
                        subject: peer_pubkey.to_bytes(),
                        sig_handle: cap_handle_raw,
                        admission,
                    });
                } else {
                    debug!(
                        limit = CAP_CONFIRMATION_QUEUE_LIMIT,
                        "delivery-confirmation queue full; dropping positive evidence until a later authentication"
                    );
                }
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
    // Blob scope gate: `OP_GET_BLOB` is filtered by blob-graph reachability
    // from local legacy mutable-pin roots allowed by the verified capability.
    // Those roots are not exposed over RPC. Unrestricted caps
    // (`granted_branches() == None`) skip the reachability filter.
    //
    // Reachability is recomputed per operation for simplicity; a per-connection
    // cache would be the obvious next optimisation.

    match op {
        OP_GET_BLOB => {
            // The opcode and hash are one authorization request. Authorize
            // only after the complete frame, with no await between this
            // boundary and snapshot access. The connection-lifetime timer may
            // already have closed the transport, but existing stream halves
            // are not assumed to be cancellation-coupled to that close.
            let Some(hash) =
                recv_live_blob_request(recv, &verified, &auth_state, connection).await?
            else {
                return Ok(());
            };
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
            match bound_outbound_blob(data, MAX_GET_BLOB_BYTES) {
                Ok(Some(data)) => {
                    debug!(hash = %hex::encode(&hash[..4]), bytes = data.len(), "OP_GET_BLOB served");
                    send_u64_be(send, data.len() as u64).await?;
                    send.write_all(&data)
                        .await
                        .map_err(|e| anyhow::anyhow!("send: {e}"))?;
                }
                Ok(None) => {
                    if !in_scope_flag {
                        warn!(hash = %hex::encode(&hash[..4]), "OP_GET_BLOB denied: out of scope");
                    } else {
                        debug!(hash = %hex::encode(&hash[..4]), "OP_GET_BLOB miss: blob not present");
                    }
                    send_u64_be(send, u64::MAX).await?;
                }
                Err(bytes) => {
                    warn!(
                        hash = %hex::encode(&hash[..4]),
                        bytes,
                        limit = MAX_GET_BLOB_BYTES,
                        "OP_GET_BLOB denied: blob exceeds transport envelope"
                    );
                    send_u64_be(send, u64::MAX).await?;
                }
            }
        }

        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cap_request_status_waits_for_the_peer_durability_receipt() {
        let (events, receiver) = mpsc::channel();
        let request_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let task_events = events.clone();
        let task_slots = Arc::clone(&request_slots);
        let task = tokio::spawn(async move {
            enqueue_cap_request(
                &task_events,
                &task_slots,
                [0x81; 32],
                anybytes::Bytes::from_source(b"partial-cap".to_vec()),
            )
            .await
        });
        tokio::task::yield_now().await;

        let event = receiver
            .try_recv()
            .expect("request reaches the synchronous Peer queue");
        let NetEvent::CapRequest {
            completion,
            admission: _admission,
            ..
        } = event
        else {
            panic!("expected capability request")
        };
        assert!(
            !task.is_finished(),
            "queue admission alone must not produce a wire status"
        );

        completion.send(true).expect("host still awaits receipt");
        assert_eq!(task.await.unwrap(), crate::handshake::STATUS_OK);
    }

    #[tokio::test]
    async fn cap_request_policy_refusal_receipt_is_rejected() {
        let (events, receiver) = mpsc::channel();
        let request_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let task_events = events.clone();
        let task_slots = Arc::clone(&request_slots);
        let task = tokio::spawn(async move {
            enqueue_cap_request(
                &task_events,
                &task_slots,
                [0x82; 32],
                anybytes::Bytes::from_source(b"partial-cap".to_vec()),
            )
            .await
        });
        tokio::task::yield_now().await;

        let event = receiver
            .try_recv()
            .expect("request reaches the synchronous Peer queue");
        let NetEvent::CapRequest { completion, .. } = event else {
            panic!("expected capability request")
        };
        completion.send(false).expect("host still awaits receipt");

        assert_eq!(task.await.unwrap(), crate::handshake::STATUS_REJECTED);
    }

    #[tokio::test]
    async fn cap_request_dropped_completion_is_indeterminate() {
        let (events, receiver) = mpsc::channel();
        let request_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let task_events = events.clone();
        let task_slots = Arc::clone(&request_slots);
        let task = tokio::spawn(async move {
            enqueue_cap_request(
                &task_events,
                &task_slots,
                [0x83; 32],
                anybytes::Bytes::from_source(b"partial-cap".to_vec()),
            )
            .await
        });
        tokio::task::yield_now().await;

        let event = receiver
            .try_recv()
            .expect("request reaches the synchronous Peer queue");
        let NetEvent::CapRequest { completion, .. } = event else {
            panic!("expected capability request")
        };
        drop(completion);

        assert_eq!(task.await.unwrap(), crate::handshake::STATUS_INDETERMINATE);
    }

    #[tokio::test]
    async fn cap_request_pre_policy_admission_failures_are_rejected() {
        let (events, receiver) = mpsc::channel();
        let request_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let _held = request_slots
            .clone()
            .try_acquire_owned()
            .expect("occupy the only policy queue slot");

        assert_eq!(
            enqueue_cap_request(
                &events,
                &request_slots,
                [0x84; 32],
                anybytes::Bytes::from_source(b"partial-cap".to_vec()),
            )
            .await,
            crate::handshake::STATUS_REJECTED,
            "queue refusal happens before policy and is definitive"
        );
        assert!(receiver.try_recv().is_err());

        let (events, receiver) = mpsc::channel();
        drop(receiver);
        assert_eq!(
            enqueue_cap_request(
                &events,
                &Arc::new(tokio::sync::Semaphore::new(1)),
                [0x85; 32],
                anybytes::Bytes::from_source(b"partial-cap".to_vec()),
            )
            .await,
            crate::handshake::STATUS_REJECTED,
            "an event that never entered the policy loop is definitely refused"
        );
    }

    #[derive(Clone)]
    struct CloseProbe(Arc<std::sync::atomic::AtomicBool>);

    impl Conn for CloseProbe {
        type SendHalf = tokio::io::DuplexStream;
        type RecvHalf = tokio::io::DuplexStream;

        fn remote_id(&self) -> PeerId {
            [0xA5; 32]
        }

        fn open_bi(
            &self,
        ) -> impl std::future::Future<Output = anyhow::Result<(Self::SendHalf, Self::RecvHalf)>> + Send
        {
            async { anyhow::bail!("unused test connection") }
        }

        fn accept_bi(
            &self,
        ) -> impl std::future::Future<Output = Option<(Self::SendHalf, Self::RecvHalf)>> + Send
        {
            async { None }
        }

        fn close(&self, _code: u32, _reason: &[u8]) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn blob_authority_is_checked_after_the_complete_request_frame() {
        use tokio::io::AsyncWriteExt;
        use triblespace_core::id::ufoid;
        use triblespace_core::repo::capability::VerifiedCapability;
        use triblespace_core::trible::TribleSet;

        let verified = VerifiedCapability {
            subject: SigningKey::from_bytes(&[0x71; 32]).verifying_key(),
            scope_root: *ufoid(),
            cap_set: TribleSet::new(),
            expires_at: crate::clock::epoch_now() - hifitime::Duration::from_seconds(1.0),
        };
        let auth_state = Arc::new(tokio::sync::RwLock::new(Some(verified.clone())));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let connection = CloseProbe(closed.clone());
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let task_auth_state = auth_state.clone();
        let task_connection = connection.clone();
        let mut task = tokio::spawn(async move {
            recv_live_blob_request(&mut reader, &verified, &task_auth_state, &task_connection).await
        });

        writer.write_all(&[0x42]).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut task)
                .await
                .is_err(),
            "an expired decision before the full hash would finish prematurely"
        );
        writer.write_all(&[0x24; 31]).await.unwrap();

        assert_eq!(task.await.unwrap().unwrap(), None);
        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(auth_state.read().await.is_none());
    }

    #[cfg(feature = "sim")]
    #[tokio::test]
    async fn exact_verification_returns_locally_satisfied_parent_in_complete_closure() {
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::id::{ExclusiveId, ufoid};
        use triblespace_core::inline::encodings::hash::Handle;
        use triblespace_core::inline::{Inline, TryToInline};
        use triblespace_core::macros::entity;
        use triblespace_core::repo::BlobStorePut;
        use triblespace_core::repo::capability::{
            PERM_ADMIN, PERM_READ, build_capability, build_founder_anchor,
        };
        use triblespace_core::repo::memoryrepo::MemoryRepo;
        use triblespace_core::trible::TribleSet;

        let root = SigningKey::from_bytes(&[71; 32]);
        let issuer = SigningKey::from_bytes(&[72; 32]);
        let subject = SigningKey::from_bytes(&[73; 32]);
        let now = crate::clock::epoch_now();
        let expiry = (now, now + hifitime::Duration::from_days(1.0))
            .try_to_inline()
            .unwrap();

        let parent_scope = *ufoid();
        let parent_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&parent_scope) @
            triblespace_core::metadata::tag: PERM_ADMIN,
        });
        let parent =
            build_founder_anchor(&root, issuer.verifying_key(), parent_scope, parent_facts)
                .unwrap();
        let child_scope = *ufoid();
        let child_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&child_scope) @
            triblespace_core::metadata::tag: PERM_READ,
        });
        let child = build_capability(
            &issuer,
            subject.verifying_key(),
            parent.clone(),
            child_scope,
            child_facts,
            expiry,
        )
        .unwrap();

        // Only the parent cap is satisfied by the local snapshot. The leaf
        // pair is supplied in-band, and no network callback should be needed.
        let mut store = MemoryRepo::default();
        let parent_cap: Inline<Handle<SimpleArchive>> = store.put(parent.0.clone()).unwrap();
        let snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>> = Arc::new(Mutex::new(Some(
            Box::new(StoreSnapshot::from_store(&mut store).unwrap()),
        )));
        let leaf_cap: Inline<Handle<SimpleArchive>> = child.0.get_handle();
        let leaf_sig: Inline<Handle<SimpleArchive>> = child.1.get_handle();
        let provided = std::collections::BTreeMap::from([
            (leaf_cap.raw, child.0.bytes.clone()),
            (leaf_sig.raw, child.1.bytes.clone()),
        ]);

        let net = crate::transport::sim::SimNet::new(
            0xC105_0A11,
            crate::transport::sim::SimConfig::default(),
        );
        let harness = net.join(subject.verifying_key().to_bytes());
        let (_verified, closure) = verify_capability_chain_exact(
            &harness.transport,
            issuer.verifying_key().to_bytes(),
            leaf_sig.raw,
            subject.verifying_key(),
            root.verifying_key(),
            &snapshot,
            provided,
        )
        .await
        .expect("locally completed chain verifies");

        assert_eq!(closure.len(), 3);
        assert!(closure.contains_key(&leaf_cap.raw));
        assert!(closure.contains_key(&leaf_sig.raw));
        assert!(
            closure.contains_key(&parent_cap.raw),
            "a parent loaded from the snapshot belongs to the returned proof closure"
        );
    }

    #[tokio::test]
    async fn authenticated_resource_permits_are_fail_fast_and_lifetime_scoped() {
        let subjects = InboundSubjectRegistry::default();
        let subject = [0x51; 32];
        let live_subject = Arc::new(
            subjects
                .try_acquire(subject)
                .expect("first connection for a TLS subject is admitted"),
        );
        assert!(
            subjects.try_acquire(subject).is_none(),
            "a duplicate subject is rejected immediately rather than queued"
        );
        let live_subject_tail = live_subject.clone();
        drop(live_subject);
        assert!(
            subjects.try_acquire(subject).is_none(),
            "admitted stream work keeps the subject reserved after the accept loop exits"
        );
        drop(live_subject_tail);
        assert!(
            subjects.try_acquire(subject).is_some(),
            "dropping the final connection/stream lease releases the subject"
        );

        let connections = Arc::new(tokio::sync::Semaphore::new(1));
        let live_connection = Arc::new(
            connections
                .clone()
                .try_acquire_owned()
                .expect("first authenticated connection is admitted"),
        );
        assert!(
            connections.clone().try_acquire_owned().is_err(),
            "another connection is rejected immediately rather than queued"
        );
        let live_connection_tail = live_connection.clone();
        drop(live_connection);
        assert!(
            connections.clone().try_acquire_owned().is_err(),
            "admitted stream work keeps the live-connection slot"
        );
        drop(live_connection_tail);
        assert!(connections.try_acquire_owned().is_ok());

        let streams = Arc::new(tokio::sync::Semaphore::new(
            POSTAUTH_STREAM_PER_CONNECTION_LIMIT,
        ));
        let global = Arc::new(tokio::sync::Semaphore::new(POSTAUTH_STREAM_LIMIT));
        let executing: Vec<_> = (0..POSTAUTH_STREAM_PER_CONNECTION_LIMIT)
            .map(|_| {
                try_acquire_postauth_stream(&streams, &global)
                    .expect("the first eight streams are admitted")
            })
            .collect();
        assert!(
            matches!(
                try_acquire_postauth_stream(&streams, &global),
                Err(PostauthAdmissionError::Connection)
            ),
            "the ninth stream is rejected without waiting or consuming global capacity"
        );
        assert_eq!(
            global.available_permits(),
            POSTAUTH_STREAM_LIMIT - POSTAUTH_STREAM_PER_CONNECTION_LIMIT
        );
        drop(executing);
        assert_eq!(
            streams.available_permits(),
            POSTAUTH_STREAM_PER_CONNECTION_LIMIT
        );
        assert_eq!(global.available_permits(), POSTAUTH_STREAM_LIMIT);

        let saturated_global = Arc::new(tokio::sync::Semaphore::new(0));
        assert!(matches!(
            try_acquire_postauth_stream(&streams, &saturated_global),
            Err(PostauthAdmissionError::Global)
        ));
        assert_eq!(
            streams.available_permits(),
            POSTAUTH_STREAM_PER_CONNECTION_LIMIT,
            "a failed nested global admission rolls the local permit back"
        );
    }

    #[test]
    fn outbound_blob_limit_is_checked_on_shared_bytes() {
        let at_limit = anybytes::Bytes::from_source(vec![0xA5u8; 8]);
        assert_eq!(
            bound_outbound_blob(Some(at_limit), 8)
                .expect("at-limit blob is transportable")
                .expect("blob is present")
                .len(),
            8
        );
        let over_limit = anybytes::Bytes::from_source(vec![0x5Au8; 9]);
        assert_eq!(bound_outbound_blob(Some(over_limit), 8), Err(9));
        assert_eq!(bound_outbound_blob(None, 8), Ok(None));
    }

    #[test]
    fn branch_scoped_serving_fails_closed_after_legacy_pin_reclassification() {
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::blob::encodings::longstring::LongString;
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::id::{ExclusiveId, Id, genid, ufoid};
        use triblespace_core::inline::Inline;
        use triblespace_core::inline::encodings::hash::Handle;
        use triblespace_core::macros::entity;
        use triblespace_core::repo::capability::{PERM_READ, VerifiedCapability, scope_branch};
        use triblespace_core::repo::memoryrepo::MemoryRepo;
        use triblespace_core::repo::{BlobStorePut, PinStore, PushResult};
        use triblespace_core::trible::TribleSet;

        let mut store = MemoryRepo::default();
        let pin_id = Id::new([7; 16]).expect("nonzero pin id");
        let name: Inline<Handle<LongString>> = store
            .put("main".to_owned().to_blob())
            .expect("store branch name");

        let old_entity = genid();
        let old_tag = genid();
        let old_content = TribleSet::from(entity! {
            ExclusiveId::force_ref(&old_entity) @
            triblespace_core::metadata::tag: *old_tag,
        })
        .to_blob();
        let old_content_handle: Inline<Handle<SimpleArchive>> =
            store.put(old_content.clone()).expect("store old content");
        let legacy_metadata =
            triblespace_core::repo::branch::branch_unsigned(pin_id, name, Some(old_content));
        let legacy_head: Inline<Handle<SimpleArchive>> =
            store.put(legacy_metadata).expect("store legacy metadata");
        assert!(matches!(
            store.update(pin_id, None, Some(legacy_head)),
            Ok(PushResult::Success())
        ));

        let scope_root = ufoid();
        let mut cap_set: TribleSet = entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag: PERM_READ,
        }
        .into();
        cap_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            scope_branch: pin_id,
        });
        let verified = VerifiedCapability {
            subject: SigningKey::from_bytes(&[91; 32]).verifying_key(),
            scope_root: *scope_root,
            cap_set,
            expires_at: crate::clock::epoch_now() + hifitime::Duration::from_days(1.0),
        };

        let legacy_snapshot = StoreSnapshot::from_store(&mut store).expect("legacy snapshot");
        assert!(blob_in_scope(&legacy_snapshot, &verified, &legacy_head.raw));
        assert!(blob_in_scope(
            &legacy_snapshot,
            &verified,
            &old_content_handle.raw
        ));

        let new_entity = genid();
        let new_tag = genid();
        let new_content = TribleSet::from(entity! {
            ExclusiveId::force_ref(&new_entity) @
            triblespace_core::metadata::tag: *new_tag,
        })
        .to_blob();
        let new_content_handle: Inline<Handle<SimpleArchive>> = store
            .put(new_content.clone())
            .expect("store local-only content");
        let mut local_metadata =
            triblespace_core::repo::branch::branch_unsigned(pin_id, name, Some(new_content));
        let marker = genid();
        local_metadata += TribleSet::from(entity! {
            ExclusiveId::force_ref(&marker) @
            crate::policy::local_only_pin: crate::policy::KIND_OUTBOUND_CAP_REQUEST,
        });
        let local_head: Inline<Handle<SimpleArchive>> = store
            .put(local_metadata)
            .expect("store local-only metadata");
        assert!(matches!(
            store.update(pin_id, Some(legacy_head), Some(local_head)),
            Ok(PushResult::Success())
        ));

        let local_snapshot = StoreSnapshot::from_store(&mut store).expect("local snapshot");
        for hash in [
            legacy_head.raw,
            old_content_handle.raw,
            local_head.raw,
            new_content_handle.raw,
        ] {
            assert!(
                !blob_in_scope(&local_snapshot, &verified, &hash),
                "reclassified local-only pin must not remain a scoped serving root"
            );
        }
    }

    #[test]
    fn dht_providers_exclude_self_attempted_peer_and_duplicates() {
        let me = [1; 32];
        let publisher = [2; 32];
        let cache_a = [3; 32];
        let cache_b = [4; 32];
        assert_eq!(
            dht_provider_candidates(me, publisher, [me, cache_a, publisher, cache_a, cache_b],),
            vec![cache_a, cache_b]
        );
        assert_eq!(
            dht_provider_candidates(me, me, [me, cache_a, cache_a]),
            vec![cache_a]
        );

        let many = (10u8..30).map(|byte| [byte; 32]);
        assert_eq!(
            dht_provider_candidates(me, publisher, many),
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
    let Some(granted_branches) = verified.granted_branches() else {
        // Unrestricted cap: every blob present in the snapshot is in
        // scope. The cap may still lack read permission entirely; callers
        // cross-check `verified.grants_read()` before serving a blob.
        return None;
    };

    let mut frontier: Vec<RawHash> = granted_branches
        .into_iter()
        .filter(|id| verified.grants_read_on(id))
        .filter_map(|id| snap.legacy_head(&id.into()))
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
