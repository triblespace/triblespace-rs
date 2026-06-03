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

use iroh_base::{EndpointAddr, EndpointId};
use ed25519_dalek::SigningKey;
use tracing::{debug, debug_span, error, info, info_span, instrument, trace, warn, Instrument};

use crate::channel::{NetCommand, NetEvent, PublisherKey};
use crate::identity::iroh_secret;
use crate::protocol::*;

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
    /// Whether to subscribe to live HEAD-update gossip. The topic id
    /// is the team root pubkey's 32 bytes — every team has exactly
    /// one gossip mesh, derived from its identity. `false` = serve-
    /// /pull-only (no subscription, no broadcasts).
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
    /// this node publishes its own HEADs (write side) and/or reacts to
    /// incoming HEADs from peers (read side). Default is
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
    /// Subscribe to gossip + fetch closures AND publish our own
    /// HEADs. Default behaviour.
    #[default]
    Bidirectional,
    /// Subscribe to gossip + fetch closures, but suppress local
    /// HEAD publishes. Useful for follower / leecher workflows
    /// where the local node is catching up to the swarm and has
    /// no canonical state to contribute.
    ReadOnly,
    /// Publish local HEADs to gossip, but ignore incoming HEAD
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
    pub branches: Vec<(RawPinId, RawHash)>,
}

impl StoreSnapshot<()> {
    pub fn from_store<S>(store: &mut S) -> Option<StoreSnapshot<S::Reader>>
    where
        S: triblespace_core::repo::BlobStore
            + triblespace_core::repo::PinStore,
    {
        let ids: Vec<triblespace_core::id::Id> = store.pins().ok()?
            .filter_map(|r| r.ok())
            .collect();
        let mut branches = Vec::new();
        for id in ids {
            if let Ok(Some(head)) = store.head(id) {
                let id_bytes: [u8; 16] = id.into();
                branches.push((id_bytes, head.raw));
            }
        }
        let reader = store.reader().ok()?;
        Some(StoreSnapshot { reader, branches })
    }
}

/// Type-erased snapshot for the host thread.
///
/// Carries just enough of the pile for the network thread to serve
/// peer requests: per-hash blob fetch, branch head listing, and a
/// quick presence check.
pub trait AnySnapshot: Send + 'static {
    fn get_blob(&self, hash: &RawHash) -> Option<Vec<u8>>;
    fn has_blob(&self, hash: &RawHash) -> bool;
    fn list_branches(&self) -> &[(RawPinId, RawHash)];
}

impl<R> AnySnapshot for StoreSnapshot<R>
where
    R: triblespace_core::repo::BlobStoreGet
        + triblespace_core::repo::BlobStoreList
        + Send + 'static,
{
    fn get_blob(&self, hash: &RawHash) -> Option<Vec<u8>> {
        use triblespace_core::blob::encodings::UnknownBlob;
        use triblespace_core::inline::Inline;
        use triblespace_core::inline::encodings::hash::Handle;
        let handle = Inline::<Handle<UnknownBlob>>::new(*hash);
        self.reader.get::<anybytes::Bytes, UnknownBlob>(handle).ok().map(|b| b.to_vec())
    }

    fn has_blob(&self, hash: &RawHash) -> bool {
        self.get_blob(hash).is_some()
    }

    fn list_branches(&self) -> &[(RawPinId, RawHash)] {
        &self.branches
    }
}

// ── Outgoing half ────────────────────────────────────────────────────

/// Send commands to the host thread + update the serving snapshot.
///
/// Holds only the snapshot and the command channel — `update_snapshot`
/// is a pure snapshot refresh.
#[derive(Clone)]
pub struct NetSender {
    cmd_tx: mpsc::Sender<NetCommand>,
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    id: EndpointId,
}

impl NetSender {
    pub fn id(&self) -> EndpointId { self.id }

    pub fn announce(&self, hash: RawHash) {
        let _ = self.cmd_tx.send(NetCommand::Announce(hash));
    }

    pub fn gossip(&self, branch: RawPinId, head: RawHash) {
        let _ = self.cmd_tx.send(NetCommand::Gossip { branch, head });
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

/// Spawn the network thread. Returns the outgoing/incoming channel halves
/// — used internally by [`Peer::new`](crate::peer::Peer::new).
pub fn spawn(key: SigningKey, config: PeerConfig) -> (NetSender, NetReceiver) {
    let secret = iroh_secret(&key);
    let id: EndpointId = secret.public().into();

    let (cmd_tx, cmd_rx) = mpsc::channel::<NetCommand>();
    let (evt_tx, evt_rx) = mpsc::channel::<NetEvent>();

    let snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>> =
        Arc::new(Mutex::new(None));
    let thread_snapshot = snapshot.clone();

    let _thread = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(host_loop(
            secret,
            config,
            cmd_rx,
            evt_tx,
            thread_snapshot,
        ));
    });

    let sender = NetSender {
        cmd_tx,
        snapshot,
        id,
    };
    let receiver = NetReceiver { evt_rx };
    (sender, receiver)
}

// ── Network thread event loop ────────────────────────────────────────

/// Connect to a peer over the pile-sync ALPN and immediately present
/// our capability so subsequent ops are authorised. Protocol v4 makes
/// this mandatory — the server rejects any op until the connection
/// completes auth.
#[instrument(level = "info", skip(ep, self_cap), fields(peer = %peer.id.fmt_short()))]
async fn connect_authed(
    ep: &iroh::Endpoint,
    peer: EndpointAddr,
    self_cap: &RawHash,
) -> anyhow::Result<iroh::endpoint::Connection> {
    debug!(alpn = %String::from_utf8_lossy(PILE_SYNC_ALPN), "connecting");
    let conn = ep.connect(peer, PILE_SYNC_ALPN).await
        .map_err(|e| {
            warn!(error = %e, "connect failed");
            anyhow::anyhow!("connect: {e}")
        })?;
    debug!(self_cap = %hex::encode(&self_cap[..4]), "connected; sending OP_AUTH");
    op_auth(&conn, self_cap).await
        .map_err(|e| {
            warn!(error = %e, "auth handshake failed");
            anyhow::anyhow!("auth: {e}")
        })?;
    info!("auth ok");
    Ok(conn)
}

#[allow(clippy::too_many_arguments)]
async fn host_loop(
    secret: iroh_base::SecretKey,
    config: PeerConfig,
    commands: mpsc::Receiver<NetCommand>,
    events: mpsc::Sender<NetEvent>,
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
) {
    use iroh::endpoint::presets;
    use iroh::protocol::Router;
    use iroh::Endpoint;
    use iroh_gossip::Gossip;
    use iroh_gossip::api::GossipSender;
    use futures::TryStreamExt;

    // Use the OS trust store (via rustls-platform-verifier) rather
    // than the compiled-in Mozilla webpki-roots bundle. The default
    // (webpki-roots) breaks when running inside corporate-proxy /
    // sandbox environments that present a custom CA at egress: iroh's
    // relay HTTPS probes and pkarr publish/lookup over HTTPS get
    // `invalid peer certificate: UnknownIssuer`, discovery dies
    // silently, and the QUIC handshake never starts. Reading the OS
    // store at runtime lets the sandbox CA (or any admin-installed
    // root) participate. macOS uses the Security framework; Linux
    // reads /etc/ssl/certs; Windows reads the certificate store.

    // Strip trailing dots from default relay hostnames. iroh's
    // defaults ship FQDN-absolute form (`*.iroh-canary.iroh.link.`
    // — note trailing dot), which is technically RFC-correct but
    // propagates into reqwest's HTTP `Host` header. Many WAFs
    // (notably the one fronting Anthropic's web sandbox egress)
    // treat trailing-dot Host as a known bypass-attempt
    // signature and 503 the request, leaving iroh's net_report
    // permanently stuck. Stripping the dot before iroh constructs
    // its RelayUrls produces an HTTP-canonical Host header that
    // passes through unmolested. Same upstream relay, just
    // friendlier URL shape.
    let relay_map = dot_stripped_default_relay_map();

    // Discovery is layered. `presets::N0` gives us pkarr publish +
    // DNS lookup via n0.computer. On top of that:
    //
    // - `MdnsAddressLookup` adds local-network discovery (zero-conf,
    //   no internet needed). Two peers on the same LAN find each
    //   other without pkarr/DNS roundtrips. Works on home WiFi,
    //   conference rooms, sneakernet; subject to whether the network
    //   permits client-to-client multicast (some hostile APs filter
    //   mDNS).
    //
    // - `DhtAddressLookup` (pkarr-over-BitTorrent-DHT) gives a third
    //   discovery path that doesn't depend on n0.computer's DNS
    //   server being reachable. Default filter is `relay_only`, so
    //   we don't leak direct-IP addresses to the public DHT.
    //
    // All three providers run in parallel; lookup results are
    // unioned. If any one path is reachable, peers can find each
    // other.
    let mdns = match iroh::address_lookup::MdnsAddressLookup::builder().build(EndpointId::from(secret.public())) {
        Ok(m) => Some(m),
        Err(e) => { warn!(error = %e, "mDNS discovery init failed; continuing without LAN discovery"); None }
    };

    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret.clone())
        .ca_roots_config(iroh::tls::CaRootsConfig::system())
        .relay_mode(iroh::RelayMode::Custom(relay_map))
        .address_lookup(iroh::address_lookup::DhtAddressLookup::builder());
    if let Some(m) = mdns {
        builder = builder.address_lookup(m);
    }
    let ep = match builder.bind().await {
        Ok(ep) => ep,
        Err(e) => { error!(error = %e, "iroh endpoint bind failed; net thread exiting"); return; }
    };
    ep.online().await;

    let my_id = ep.id();
    let self_cap: RawHash = config.self_cap;
    let mut router_builder = Router::builder(ep.clone());

    // Host-wide singleflight connection pool — one authed
    // connection per remote peer, reused across all concurrent
    // fetch_reachable / swarm_fetch_chain calls. See `SharedPool`
    // docs for the OnceCell-based dial deduplication. Named
    // `conn_pool` to avoid shadowing the unrelated iroh_blobs
    // ConnectionPool that gets initialized below for the DHT.
    let conn_pool: SharedPool = new_shared_pool();

    // DHT — always on. Peers bootstrap the routing table.
    let dht_alpn = crate::dht::rpc::ALPN;
    let pool = iroh_blobs::util::connection_pool::ConnectionPool::new(
        ep.clone(), dht_alpn,
        iroh_blobs::util::connection_pool::Options {
            max_connections: 64,
            idle_timeout: std::time::Duration::from_secs(30),
            connect_timeout: std::time::Duration::from_secs(10),
            on_connected: None,
        },
    );
    let iroh_pool = crate::dht::pool::IrohPool::new(ep.clone(), pool);
    // Gossip + DHT bootstrap want bare EndpointIds; the addresses
    // attached to each peer were seeded into the address lookup
    // service above, so iroh will resolve them locally on connect.
    let bootstrap_ids: Vec<EndpointId> =
        config.peers.iter().map(|addr| addr.id).collect();
    let (rpc, dht_api) = crate::dht::create_node(
        my_id, iroh_pool.clone(), bootstrap_ids.clone(), Default::default(),
    );
    iroh_pool.set_self_client(Some(rpc.downgrade()));
    let dht_sender = rpc.inner().as_local().expect("local sender");
    router_builder = router_builder
        .accept(dht_alpn, irpc_iroh::IrohProtocol::with_sender(dht_sender));
    let dht_api = Some(dht_api);

    // Protocol handler. ep + dht + self_cap + events are threaded
    // through so the OP_AUTH path can fall back to a swarm fetch
    // when the presented cap chain references blobs we don't have
    // locally (caps are orphan blobs that don't ride along with
    // normal branch syncs).
    let handler = SnapshotHandler {
        snapshot: snapshot.clone(),
        team_root: config.team_root,
        ep: ep.clone(),
        dht: dht_api.clone(),
        self_cap,
        events: events.clone(),
        pool: conn_pool.clone(),
        my_id,
    };
    router_builder = router_builder.accept(PILE_SYNC_ALPN, handler);

    // Auth-handshake ALPN (separate from PILE_SYNC because it's open
    // to unauthenticated peers — a peer requesting their first cap
    // has nothing to present at OP_AUTH yet). The handler forwards
    // incoming requests/deliveries to the Peer's event channel; the
    // Peer surfaces them via NetEvent::CapRequest / CapDelivered.
    let handshake_handler = HandshakeHandler {
        events: events.clone(),
    };
    router_builder = router_builder
        .accept(crate::handshake::AUTH_HANDSHAKE_ALPN, handshake_handler);

    // Gossip.
    let mut gossip_sender: Option<GossipSender> = None;
    if config.gossip {
        let gossip = Gossip::builder().spawn(ep.clone());
        router_builder = router_builder.accept(iroh_gossip::ALPN, gossip.clone());

        // Topic id is the team root pubkey directly: the team root is
        // already 32 uniform bytes (an ed25519 pubkey), so no hashing
        // is needed. One gossip mesh per team — knowing the team
        // identifies the rendezvous channel.
        let topic_id = iroh_gossip::TopicId::from_bytes(config.team_root.to_bytes());
        // Always use subscribe (non-blocking). The join happens in the background
        // as peers come online. subscribe_and_join blocks until at least one peer
        // is reachable, which causes hangs if peers start at different times.
        let topic = gossip.subscribe(topic_id, bootstrap_ids.clone()).await;
        if let Ok(topic) = topic {
            let (sender, receiver) = topic.split();
            gossip_sender = Some(sender);
            let events_tx = events.clone();
            let ep2 = ep.clone();
            let dht_api2 = dht_api.clone();
            // Local snapshot handle — used by fetch_reachable's
            // discovery phase to skip subtrees we already have.
            // Same Arc the protocol server uses to answer
            // OP_CHILDREN / OP_GET_BLOB to remote peers.
            let snapshot_for_fetch = snapshot.clone();
            let pool_for_fetch = conn_pool.clone();
            let my_id_for_fetch = my_id;
            tokio::spawn(async move {
                let mut receiver = receiver;
                while let Ok(Some(event)) = receiver.try_next().await {
                    match &event {
                        iroh_gossip::api::Event::Received(msg) => {
                            // Gossip HEAD message: 0x01 + branch(16) + head(32) + publisher(32) = 81 bytes
                            if msg.content.len() == 81 && msg.content[0] == 0x01 {
                                let mut branch = [0u8; 16];
                                branch.copy_from_slice(&msg.content[1..17]);
                                let mut head = [0u8; 32];
                                head.copy_from_slice(&msg.content[17..49]);
                                let mut publisher = [0u8; 32];
                                publisher.copy_from_slice(&msg.content[49..81]);

                                let ep2 = ep2.clone();
                                let events_tx2 = events_tx.clone();
                                let dht2 = dht_api2.clone();
                                let self_cap2 = self_cap;
                                let snap2 = snapshot_for_fetch.clone();
                                let pool2 = pool_for_fetch.clone();
                                let my_id2 = my_id_for_fetch;
                                // Use publisher key to connect for fetch (they're the source).
                                let fetch_peer = if let Ok(pk) = iroh_base::PublicKey::from_bytes(&publisher) {
                                    pk.into()
                                } else {
                                    msg.delivered_from.into()
                                };
                                tokio::spawn(async move {
                                    debug!(
                                        head = %hex::encode(&head[..4]),
                                        publisher = %hex::encode(&publisher[..4]),
                                        "gossip head update; fetching"
                                    );
                                    track_known_head(&ep2, fetch_peer, branch, head, publisher, &dht2, &events_tx2, &self_cap2, &snap2, &pool2, my_id2).await;
                                });
                            }
                        }
                        iroh_gossip::api::Event::NeighborUp(peer) => {
                            info!(peer = %peer.fmt_short(), "gossip neighbor up");
                        }
                        iroh_gossip::api::Event::NeighborDown(peer) => {
                            info!(peer = %peer.fmt_short(), "gossip neighbor down");
                        }
                        _ => {}
                    }
                }
            });
        }
    }

    let _router = router_builder.spawn();

    /// Build the gossip wire frame for a (branch, head) pair.
    /// 0x01 | branch(16) | head(32) | publisher(32) = 81 bytes.
    fn gossip_frame(branch: &RawPinId, head: &RawHash, publisher: &EndpointId) -> Vec<u8> {
        let mut msg = Vec::with_capacity(81);
        msg.push(0x01);
        msg.extend_from_slice(branch);
        msg.extend_from_slice(head);
        msg.extend_from_slice(publisher.as_bytes());
        msg
    }

    // Last published HEAD per branch. Lets the periodic
    // re-broadcast tick replay our state without callers
    // having to drive it. iroh-gossip dedupes identical
    // frames, so replaying the same set every 30s is cheap
    // for neighbors who've already seen it, while giving
    // newly-joined neighbors a chance to discover our HEADs
    // without a JOIN message (which would add a DOS surface).
    let mut last_published: HashMap<RawPinId, RawHash> = HashMap::new();
    let rebroadcast_period = std::time::Duration::from_secs(30);
    let mut last_rebroadcast = std::time::Instant::now();

    // Command loop.
    loop {
        while let Ok(cmd) = commands.try_recv() {
            match cmd {
                NetCommand::Announce(hash) => {
                    if let Some(api) = &dht_api {
                        let api = api.clone();
                        tokio::spawn(async move {
                            let blake3_hash = blake3::Hash::from_bytes(hash);
                            let _ = api.announce_provider(blake3_hash, my_id).await;
                        });
                    }
                }
                NetCommand::Gossip { branch, head } => {
                    last_published.insert(branch, head);
                    if let Some(sender) = &gossip_sender {
                        let msg = gossip_frame(&branch, &head, &my_id);
                        let sender = sender.clone();
                        tokio::spawn(async move {
                            let _ = sender.broadcast(msg.into()).await;
                        });
                    }
                }
                NetCommand::DeliverCap { subject, cap_bytes, sig_bytes } => {
                    // Open a fresh connection on the auth-handshake
                    // ALPN, send OP_DELIVER_CAP, close. The recipient's
                    // ack byte is observed but not surfaced — the
                    // command is fire-and-forget at the Peer API
                    // level. Failure paths (connection refused, peer
                    // unreachable, recipient rejected) just log; the
                    // renewal-daemon retries on its next tick.
                    let ep_for_deliver = ep.clone();
                    tokio::spawn(async move {
                        let subject_id = match iroh_base::EndpointId::from_bytes(&subject) {
                            Ok(id) => id,
                            Err(e) => {
                                warn!(error = %e, "DeliverCap: bad subject pubkey");
                                return;
                            }
                        };
                        let conn = match ep_for_deliver
                            .connect(
                                subject_id,
                                crate::handshake::AUTH_HANDSHAKE_ALPN,
                            )
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
                        match crate::handshake::send_deliver_cap(
                            &conn, &cap_bytes, &sig_bytes,
                        )
                        .await
                        {
                            Ok(status) if status == crate::handshake::STATUS_OK => {
                                debug!(
                                    subject = %hex::encode(&subject[..4]),
                                    "DeliverCap: recipient ack OK"
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
                        conn.close(0u32.into(), b"ok");
                    });
                }
            }
        }

        if last_rebroadcast.elapsed() >= rebroadcast_period {
            if let Some(sender) = &gossip_sender {
                for (branch, head) in &last_published {
                    let msg = gossip_frame(branch, head, &my_id);
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let _ = sender.broadcast(msg.into()).await;
                    });
                }
            }
            last_rebroadcast = std::time::Instant::now();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Fetch all blobs reachable from a HEAD, swarm-distributed.
///
/// For each blob along the BFS, asks the DHT for providers and
/// fans the fetch across whoever's reachable; falls back to the
/// gossip publisher if DHT lookup is empty. A per-pull connection
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
/// number of ops; with DHT-driven provider selection, the walk
/// fans out across multiple caching peers in parallel hops
/// rather than funnelling everything through the publisher.
#[allow(clippy::too_many_arguments)]
async fn fetch_reachable(
    ep: &iroh::Endpoint,
    publisher: EndpointAddr,
    head: &RawHash,
    dht: &Option<crate::dht::api::ApiClient>,
    events: &mpsc::Sender<NetEvent>,
    self_cap: &RawHash,
    local: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: &SharedPool,
    my_id: EndpointId,
) -> anyhow::Result<()> {
    // Local-presence check against the same snapshot the server
    // uses to answer remote OP_CHILDREN / OP_GET_BLOB. Closure
    // (rather than inline lookups) so the lock-and-snap-deref
    // dance lives in one place.
    let have_local = |hash: &RawHash| -> bool {
        local
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.has_blob(hash))
            .unwrap_or(false)
    };

    // Short-circuit: if the HEAD is already local, the bottom-up
    // insertion invariant guarantees its whole closure is local
    // too (Phase 2 writes children before parents; a stored blob
    // implies stored children). Caught-up gossip rebroadcasts hit
    // this case and incur zero wire bytes.
    if have_local(head) {
        return Ok(());
    }

    let publisher_id = publisher.id;

    // Seed the pool with the publisher's connection on first encounter.
    // pool_get is singleflight-on-dial via OnceCell, so concurrent
    // fetch_reachable calls targeting the same publisher share one
    // dial and one OP_AUTH; the resulting connection serves every
    // op_children/op_get_blob on this and all other walks.
    // Note we pass the *fully-addressed* publisher only to seed the
    // cell with a known-good address; subsequent pool_get calls fall
    // through to iroh's address lookup if needed.
    trace!(head = %hex::encode(&head[..4]), publisher = %publisher_id.fmt_short(), "fetch_reachable: seeding pool");
    let _ = pool_get(ep, pool, publisher_id, self_cap).await;
    trace!(head = %hex::encode(&head[..4]), "fetch_reachable: pool seeded; entering Phase 1");

    // ── Phase 1: discovery (OP_CHILDREN only) ──
    //
    // Walk the closure top-down via OP_CHILDREN. For each child,
    // skip if already local (the subtree is guaranteed present by
    // the bottom-up insertion invariant; descending would be
    // wasted wire bytes). Build `to_fetch` in BFS order so reverse
    // iteration in Phase 2 gives bottom-up arrival to the store.
    let mut seen: HashSet<RawHash> = HashSet::new();
    let mut to_fetch: Vec<RawHash> = Vec::new();
    let mut frontier: Vec<RawHash> = vec![*head];
    seen.insert(*head);
    to_fetch.push(*head);

    while !frontier.is_empty() {
        let mut next: Vec<RawHash> = Vec::new();
        for parent in &frontier {
            trace!(parent = %hex::encode(&parent[..4]), "fetch_reachable: calling children_one");
            let children = match children_one(ep, parent, dht, pool, publisher_id, my_id, self_cap).await {
                Some(c) => c,
                None => {
                    warn!(parent = %hex::encode(&parent[..4]), "op_children: no provider could serve");
                    continue;
                }
            };
            trace!(parent = %hex::encode(&parent[..4]), n = children.len(), "fetch_reachable: children_one returned");
            for hash in children {
                if !seen.insert(hash) {
                    continue;
                }
                // The first time we see a hash determines whether
                // it ends up in to_fetch. If we already have it
                // locally, the closure below it is also local
                // (invariant), so don't enqueue or descend.
                if have_local(&hash) {
                    continue;
                }
                to_fetch.push(hash);
                next.push(hash);
            }
        }
        frontier = next;
    }

    // ── Phase 2: transfer (OP_GET_BLOB, deepest-first) ──
    //
    // Reverse BFS order = bottom-up: emit children before parents.
    // Peer's mpsc receiver preserves order, so by the time it puts
    // any parent into the store, its discovered-and-fetched children
    // are already in; blobs that *weren't* discovered (have_local
    // short-circuited them in Phase 1) were already locally present
    // before Phase 1 started — and the same invariant said their
    // closures were too.
    //
    // **Abort on first fetch failure.** If we can't fetch a child,
    // we must NOT proceed to fetch its parents — writing a parent
    // whose closure is incomplete would break the "stored blob ⇒
    // closure stored" invariant that the have_local short-circuit
    // relies on. Worse, append-only storage means any incomplete
    // parent we wrote stays in the pile forever; Phase 1 would then
    // short-circuit on that broken parent on every future sync, so
    // the gap becomes permanent.
    //
    // Aborting drops the current walk's tracking-pin update too
    // (the caller only emits NetEvent::Head on Ok), so on the next
    // gossip rebroadcast Phase 1 re-walks from the head. Whatever
    // descendants we *did* successfully write before the failure
    // remain valid (they're deeper in the BFS, so by reverse-order
    // they were completed before we hit the failure); Phase 1 will
    // short-circuit on them and only re-fetch the still-missing
    // ancestors.
    for hash in to_fetch.iter().rev() {
        let Some(data) = fetch_one(ep, hash, dht, pool, publisher_id, my_id, self_cap).await
        else {
            warn!(
                hash = %hex::encode(&hash[..4]),
                "fetch aborted: blob unavailable; head not advanced (will retry on next gossip)"
            );
            return Err(anyhow::anyhow!(
                "blob unavailable from all known providers: {}",
                hex::encode(hash)
            ));
        };
        if blake3::hash(&data).as_bytes() != hash {
            warn!(
                hash = %hex::encode(&hash[..4]),
                "fetch aborted: hash mismatch; head not advanced"
            );
            return Err(anyhow::anyhow!(
                "hash mismatch on fetched blob: expected {}",
                hex::encode(hash)
            ));
        }
        let _ = events.send(NetEvent::Blob(anybytes::Bytes::from_source(data)));
    }

    // No close: connections live in the shared pool for the
    // host_loop's lifetime, reused by subsequent walks.
    Ok(())
}

/// Resolve providers for a hash via DHT, append the publisher as a
/// fallback if it's not already in the set. Returns the ordered
/// candidate list — DHT providers first (likely caching peers,
/// closer in the swarm), publisher last (always-available fallback).
///
/// Self is filtered out — `find_providers` will list us as a
/// provider for any blob we've announced, and trying to dial
/// ourselves trips iroh's "Connecting to ourself is not supported"
/// error. If we have the blob, we'd have hit the `have_local`
/// short-circuit upstream; if we're being asked to fetch, by
/// definition we don't have it (yet) — so self is never useful here.
async fn providers_for(
    hash: &RawHash,
    dht: &Option<crate::dht::api::ApiClient>,
    publisher_id: EndpointId,
    my_id: EndpointId,
) -> Vec<EndpointId> {
    let mut providers: Vec<EndpointId> = if let Some(api) = dht {
        let blake3_hash = blake3::Hash::from_bytes(*hash);
        trace!(hash = %hex::encode(&hash[..4]), "providers_for: DHT find_providers awaiting");
        let result = api.find_providers(blake3_hash).await;
        match &result {
            Ok(v) => trace!(hash = %hex::encode(&hash[..4]), n = v.len(), "providers_for: DHT find_providers returned"),
            Err(e) => trace!(hash = %hex::encode(&hash[..4]), error = %e, "providers_for: DHT find_providers errored"),
        }
        result.unwrap_or_default()
    } else {
        trace!(hash = %hex::encode(&hash[..4]), "providers_for: no DHT");
        Vec::new()
    };
    providers.retain(|id| *id != my_id);
    if publisher_id != my_id && !providers.contains(&publisher_id) {
        providers.push(publisher_id);
    }
    providers
}

/// Host-wide connection pool: one authed `iroh::endpoint::Connection`
/// per remote peer, shared across all concurrent `fetch_reachable` /
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
pub(crate) type SharedPool = Arc<tokio::sync::Mutex<
    HashMap<EndpointId, Arc<tokio::sync::OnceCell<iroh::endpoint::Connection>>>,
>>;

fn new_shared_pool() -> SharedPool {
    Arc::new(tokio::sync::Mutex::new(HashMap::new()))
}

/// Get-or-dial an authed connection to `provider` from the shared
/// pool. `OnceCell::get_or_try_init` runs the dial exactly once even
/// if many tasks race here concurrently; the rest await the same
/// initialization. Returns `None` if the dial fails (the cell stays
/// uninitialized so a later call can retry).
async fn pool_get(
    ep: &iroh::Endpoint,
    pool: &SharedPool,
    provider: EndpointId,
    self_cap: &RawHash,
) -> Option<iroh::endpoint::Connection> {
    let cell = {
        let mut guard = pool.lock().await;
        guard
            .entry(provider)
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone()
    };
    let init = || async {
        let addr: EndpointAddr = provider.into();
        connect_authed(ep, addr, self_cap).await
    };
    match cell.get_or_try_init(init).await {
        Ok(conn) => Some(conn.clone()),
        Err(e) => {
            debug!(error = %e, provider = %provider.fmt_short(), "pool dial failed");
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
async fn pool_evict(pool: &SharedPool, provider: EndpointId) {
    let removed = {
        let mut guard = pool.lock().await;
        guard.remove(&provider)
    };
    if let Some(cell) = removed {
        if let Some(conn) = cell.get() {
            conn.close(0u32.into(), b"pool evict");
        }
    }
}

/// Fetch a single blob via the swarm — DHT-resolved providers
/// first, publisher as fallback. Returns the first successful
/// fetch's bytes (caller verifies hash).
#[allow(clippy::too_many_arguments)]
async fn fetch_one(
    ep: &iroh::Endpoint,
    hash: &RawHash,
    dht: &Option<crate::dht::api::ApiClient>,
    pool: &SharedPool,
    publisher_id: EndpointId,
    my_id: EndpointId,
    self_cap: &RawHash,
) -> Option<Vec<u8>> {
    let providers = providers_for(hash, dht, publisher_id, my_id).await;
    for provider in providers {
        let Some(conn) = pool_get(ep, pool, provider, self_cap).await else {
            continue;
        };
        match op_get_blob(&conn, hash).await {
            Ok(Some(data)) => return Some(data),
            Ok(None) => {
                debug!(hash = %hex::encode(&hash[..4]), provider = %provider.fmt_short(), "blob miss");
                continue;
            }
            Err(e) => {
                debug!(error = %e, hash = %hex::encode(&hash[..4]), provider = %provider.fmt_short(), "op_get_blob errored, evicting and trying next provider");
                // Connection-level error: pooled connection may be
                // dead. Evict so subsequent ops to this peer re-dial.
                pool_evict(pool, provider).await;
                continue;
            }
        }
    }
    None
}

/// Convert the connecting peer's verified pubkey into an EndpointAddr
/// suitable for `connect_authed`. Carries no relay/direct addrs — iroh's
/// discovery layer resolves them on dial. Used by the OP_AUTH swarm-
/// fetch fallback to seed the publisher slot of the fetch pool with
/// the very peer that just initiated the OP_AUTH (they have their
/// own cap by construction).
fn peer_endpoint_for_dialer(peer_pubkey: ed25519_dalek::VerifyingKey) -> EndpointAddr {
    // iroh's PublicKey wraps the same 32 ed25519 bytes.
    let pk = iroh_base::PublicKey::from_bytes(peer_pubkey.as_bytes())
        .expect("ed25519 VerifyingKey is a valid iroh PublicKey");
    EndpointAddr::from(EndpointId::from(pk))
}

/// Swarm-fetch the closure rooted at `head` (a cap sig handle, in the
/// OP_AUTH context) and return it as a `HashMap<RawHash, Vec<u8>>`.
/// Mirrors `fetch_reachable`'s two-phase walk (Phase 1 OP_CHILDREN
/// discovery, Phase 2 OP_GET_BLOB in reverse-BFS order) but writes
/// the results to a map instead of emitting `NetEvent::Blob`. The
/// caller decides whether to cache the bytes into the local store
/// after using them.
#[allow(clippy::too_many_arguments)]
async fn swarm_fetch_chain(
    ep: &iroh::Endpoint,
    publisher: EndpointAddr,
    head: &RawHash,
    dht: &Option<crate::dht::api::ApiClient>,
    self_cap: &RawHash,
    pool: &SharedPool,
    my_id: EndpointId,
) -> HashMap<RawHash, Vec<u8>> {
    let mut fetched: HashMap<RawHash, Vec<u8>> = HashMap::new();
    let publisher_id = publisher.id;

    // Ensure we have an authed connection to the publisher (the
    // peer that just sent us the cap_handle via OP_AUTH). pool_get
    // is singleflight, so concurrent swarm_fetch_chain calls in
    // the parallel-OP_AUTH-burst case share one dial + one OP_AUTH.
    // The whole recursion bottoms out at the publisher for typical
    // two-level chains.
    if pool_get(ep, pool, publisher_id, self_cap).await.is_none() {
        // Couldn't even auth to the dialer. Give up — there's no
        // realistic path to fetch the chain without them.
        return fetched;
    }

    // Phase 1: discovery via OP_CHILDREN. BFS order; stop when
    // every frontier blob is either no-children (root cap) or
    // unreachable.
    let mut seen: HashSet<RawHash> = HashSet::new();
    let mut to_fetch: Vec<RawHash> = Vec::new();
    let mut frontier: Vec<RawHash> = vec![*head];
    seen.insert(*head);
    to_fetch.push(*head);

    while !frontier.is_empty() {
        let mut next: Vec<RawHash> = Vec::new();
        for parent in &frontier {
            let children = match children_one(ep, parent, dht, pool, publisher_id, my_id, self_cap).await {
                Some(c) => c,
                None => continue,
            };
            for hash in children {
                if !seen.insert(hash) {
                    continue;
                }
                to_fetch.push(hash);
                next.push(hash);
            }
        }
        frontier = next;
    }

    // Phase 2: deepest-first fetch. Order matters for the caller's
    // cache-write step: emitting children before parents keeps the
    // bottom-up insertion invariant when the events get drained.
    for hash in to_fetch.iter().rev() {
        let Some(data) = fetch_one(ep, hash, dht, pool, publisher_id, my_id, self_cap).await else {
            continue;
        };
        if blake3::hash(&data).as_bytes() != hash {
            warn!(hash = %hex::encode(&hash[..4]), "hash mismatch on swarm-fetched cap blob");
            continue;
        }
        fetched.insert(*hash, data);
    }

    fetched
}

/// Walk children of a parent blob via the swarm.
#[allow(clippy::too_many_arguments)]
async fn children_one(
    ep: &iroh::Endpoint,
    parent: &RawHash,
    dht: &Option<crate::dht::api::ApiClient>,
    pool: &SharedPool,
    publisher_id: EndpointId,
    my_id: EndpointId,
    self_cap: &RawHash,
) -> Option<Vec<RawHash>> {
    trace!(parent = %hex::encode(&parent[..4]), "children_one: providers_for awaiting");
    let providers = providers_for(parent, dht, publisher_id, my_id).await;
    trace!(parent = %hex::encode(&parent[..4]), n = providers.len(), "children_one: providers_for returned");
    for provider in &providers {
        trace!(parent = %hex::encode(&parent[..4]), provider = %provider.fmt_short(), "children_one: pool_get awaiting");
        let Some(conn) = pool_get(ep, pool, *provider, self_cap).await else {
            trace!(parent = %hex::encode(&parent[..4]), provider = %provider.fmt_short(), "children_one: pool_get returned None");
            continue;
        };
        trace!(parent = %hex::encode(&parent[..4]), provider = %provider.fmt_short(), "children_one: op_children awaiting");
        match op_children(&conn, parent).await {
            Ok(c) => return Some(c),
            Err(e) => {
                debug!(error = %e, parent = %hex::encode(&parent[..4]), provider = %provider.fmt_short(), "op_children errored, evicting and trying next provider");
                pool_evict(pool, *provider).await;
                continue;
            }
        }
    }
    None
}

/// Fetch the reachable closure from `head` on `fetch_peer` and, on
/// success, emit a [`NetEvent::Head`] so the Peer materializes a
/// tracking pin.
///
/// Shared tail of the gossip-arrival handler and the `Track` command:
/// both know (fetch_peer, branch, head, publisher) by the time they
/// get here. Gossip gets the head directly from the broadcast message;
/// `Track` asks the peer via `op_head` first.
#[allow(clippy::too_many_arguments)]
async fn track_known_head(
    ep: &iroh::Endpoint,
    fetch_peer: EndpointAddr,
    branch: RawPinId,
    head: RawHash,
    publisher: crate::channel::PublisherKey,
    dht: &Option<crate::dht::api::ApiClient>,
    events: &mpsc::Sender<NetEvent>,
    self_cap: &RawHash,
    local: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    pool: &SharedPool,
    my_id: EndpointId,
) {
    let fetch_id = fetch_peer.id;
    if let Err(e) = fetch_reachable(ep, fetch_peer, &head, dht, events, self_cap, local, pool, my_id).await {
        warn!(error = %e, peer = %fetch_id.fmt_short(), "fetch_reachable failed");
    } else {
        let _ = events.send(NetEvent::Head { branch, head, publisher });
    }
}

// ── Protocol handler ─────────────────────────────────────────────────

#[derive(Clone)]
struct SnapshotHandler {
    snapshot: Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    /// Verifies all incoming capability chains. Required — protocol v4
    /// has mandatory auth.
    team_root: ed25519_dalek::VerifyingKey,
    /// Our endpoint for opening outbound connections during the
    /// swarm-fetch fallback in OP_AUTH (when an incoming cap chain
    /// references blobs we don't have locally).
    ep: iroh::Endpoint,
    /// DHT client for resolving "who has this cap blob?" during the
    /// swarm-fetch fallback. `None` only in test setups that don't
    /// bring up a DHT — production always has one.
    dht: Option<crate::dht::api::ApiClient>,
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
    pool: SharedPool,
    /// Our own endpoint id. Used to filter self out of provider
    /// lists during swarm-fetch (find_providers may list us as a
    /// provider for blobs we've announced, but we'd hit the local
    /// cache instead of dialing — and iroh refuses self-connect).
    my_id: EndpointId,
}

impl std::fmt::Debug for SnapshotHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotHandler").finish()
    }
}

/// Protocol handler for `/triblespace/auth-handshake/1`. Accepts
/// incoming `OP_REQUEST_CAP` and `OP_DELIVER_CAP` streams and
/// forwards their payloads to the Peer's event channel. All policy
/// (approve / queue / reject; verify / pin / drop) lives in the
/// receiving Peer, not here — this handler just bridges the wire to
/// the local event queue.
#[derive(Clone)]
struct HandshakeHandler {
    events: mpsc::Sender<NetEvent>,
}

impl std::fmt::Debug for HandshakeHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandshakeHandler").finish()
    }
}

impl iroh::protocol::ProtocolHandler for HandshakeHandler {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), iroh::protocol::AcceptError> {
        let peer_endpoint = connection.remote_id();
        // PublisherKey is just the 32-byte pubkey representation;
        // iroh's `EndpointId` is already an ed25519 pubkey so this
        // is a direct byte extraction (matched against the type
        // alias in channel.rs).
        let peer_pubkey: PublisherKey = *peer_endpoint.as_bytes();
        let events = self.events.clone();
        let span = info_span!(
            "auth-handshake",
            peer = %peer_endpoint.fmt_short(),
        );
        async move {
            // Each connection can carry multiple bi-streams (e.g. a
            // request followed by a deliver). Loop until the peer
            // closes the connection.
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        debug!(error = %e, "accept_bi ended; handshake connection closing");
                        break;
                    }
                };
                match crate::handshake::read_incoming(&mut recv).await {
                    Ok(Some(crate::handshake::IncomingOp::Request {
                        partial_cap_bytes,
                    })) => {
                        let _ = events.send(NetEvent::CapRequest {
                            requester: peer_pubkey,
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
                        let _ = events.send(NetEvent::CapDelivered {
                            issuer: peer_pubkey,
                            cap_bytes,
                            sig_bytes,
                        });
                        let _ = crate::handshake::respond(
                            &mut send,
                            crate::handshake::STATUS_OK,
                        )
                        .await;
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
        Ok(())
    }
}

impl iroh::protocol::ProtocolHandler for SnapshotHandler {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), iroh::protocol::AcceptError> {
        let snap = self.snapshot.clone();
        let team_root = self.team_root;
        let ep = self.ep.clone();
        let dht = self.dht.clone();
        let self_cap = self.self_cap;
        let events = self.events.clone();
        let pool = self.pool.clone();
        let my_id = self.my_id;

        let peer_endpoint = connection.remote_id();
        let span = info_span!(
            "connection",
            peer = %peer_endpoint.fmt_short(),
            alpn = %String::from_utf8_lossy(PILE_SYNC_ALPN),
        );

        async move {
            info!("connection accepted");

            // Extract the connecting peer's verified ed25519 identity
            // from iroh's TLS handshake.
            let peer_pubkey = match ed25519_dalek::VerifyingKey::from_bytes(
                peer_endpoint.as_bytes(),
            ) {
                Ok(k) => k,
                Err(e) => {
                    warn!(error = %e, "peer pubkey parse failed; closing");
                    return;
                }
            };

            // Per-connection auth state. Set by the first `OP_AUTH`
            // stream; read by every subsequent stream to gate access.
            let auth_state: Arc<tokio::sync::RwLock<
                Option<triblespace_core::repo::capability::VerifiedCapability>,
            >> = Arc::new(tokio::sync::RwLock::new(None));

            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        debug!(error = %e, "accept_bi ended; connection closing");
                        break;
                    }
                };
                let snap = snap.clone();
                let auth_state = auth_state.clone();
                let ep = ep.clone();
                let dht = dht.clone();
                let events = events.clone();
                let pool = pool.clone();
                tokio::spawn(
                    async move {
                        if let Err(e) = serve_stream(
                            &snap,
                            team_root,
                            peer_pubkey,
                            auth_state,
                            &ep,
                            &dht,
                            &self_cap,
                            &events,
                            &pool,
                            my_id,
                            &mut send,
                            &mut recv,
                        ).await {
                            error!(error = %e, "stream handler error");
                        }
                        let _ = send.finish();
                    }
                    .in_current_span(),
                );
            }
        }
        .instrument(span)
        .await;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_stream(
    snap_arc: &Arc<Mutex<Option<Box<dyn AnySnapshot>>>>,
    team_root: ed25519_dalek::VerifyingKey,
    peer_pubkey: ed25519_dalek::VerifyingKey,
    auth_state: Arc<tokio::sync::RwLock<
        Option<triblespace_core::repo::capability::VerifiedCapability>,
    >>,
    ep: &iroh::Endpoint,
    dht: &Option<crate::dht::api::ApiClient>,
    self_cap: &RawHash,
    events: &mpsc::Sender<NetEvent>,
    pool: &SharedPool,
    my_id: EndpointId,
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
) -> anyhow::Result<()> {
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::inline::encodings::hash::Handle;
    use triblespace_core::inline::Inline;

    let op = recv_u8(recv).await?;
    let span = debug_span!("stream", op = op_name(op));
    let _enter = span.enter();

    if op == OP_AUTH {
        let cap_handle_raw = recv_hash(recv).await?;
        debug!(cap_handle = %hex::encode(&cap_handle_raw[..4]), "auth: cap handle received");
        let cap_handle: Inline<Handle<SimpleArchive>> =
            Inline::new(cap_handle_raw);

        // Brief sync read inside async — guard is dropped before any
        // .await runs so this never blocks an async worker.
        // First-pass verify with local-only lookup. The common case is
        // "we already have the whole chain"; only retry with a swarm
        // fetch on the specific "missing blob" failure mode.
        let verify_once = |fetched: &HashMap<RawHash, Vec<u8>>| {
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
                    Some(Blob::new(anybytes::Bytes::from_source(bytes)))
                },
            )
        };

        let mut fetched: HashMap<RawHash, Vec<u8>> = HashMap::new();
        let mut result = verify_once(&fetched);

        // Swarm fetch + retry on missing-blob. Caps are orphan blobs
        // (not reachable from any branch HEAD), so they don't ride
        // along with normal sync. On first auth from a peer whose
        // chain we haven't cached, this walks the chain via OP_CHILDREN
        // and pulls the cap blobs into a local HashMap. Sending peers
        // verify our chain when we dial them (mutual recursion that
        // terminates because the union of all members' piles holds
        // every cap that's been issued).
        if matches!(
            result,
            Err(triblespace_core::repo::capability::VerifyError::Fetch),
        ) {
            debug!(
                cap_handle = %hex::encode(&cap_handle_raw[..4]),
                "auth: chain incomplete locally, swarm-fetching",
            );
            let publisher_addr: EndpointAddr = peer_endpoint_for_dialer(peer_pubkey);
            fetched = swarm_fetch_chain(ep, publisher_addr, &cap_handle_raw, dht, self_cap, pool, my_id).await;
            debug!(blobs = fetched.len(), "swarm-fetched chain blobs");
            result = verify_once(&fetched);
        }

        match result {
            Ok(verified) => {
                let granted = verified
                    .granted_branches()
                    .map(|s| s.len())
                    .unwrap_or(0);
                let unrestricted = verified.granted_branches().is_none();
                info!(branches = granted, unrestricted = unrestricted, "auth ok");
                // Cache the swarm-fetched blobs into the local store so
                // the next AUTH involving the same chain finds them
                // locally. mpsc preserves order; child-before-parent
                // ordering doesn't matter here because the chain is
                // already self-consistent (every parent referenced by
                // every fetched cap is also in `fetched`).
                for (_, bytes) in fetched.drain() {
                    let _ = events.send(NetEvent::Blob(anybytes::Bytes::from_source(bytes)));
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
    // Two-tier scope gate:
    //
    //  - branch level: `OP_LIST` and `OP_HEAD` are filtered by
    //    `verified.grants_read_on(branch)`.
    //  - blob level: `OP_GET_BLOB` and `OP_CHILDREN` are filtered by
    //    blob-graph reachability from the allowed heads. A peer with a
    //    cap restricted to branch X cannot fetch blobs that only branch
    //    Y reaches, even if they probe by raw hash. Unrestricted caps
    //    (`granted_branches() == None`) skip the reachability filter.
    //
    // Reachability is recomputed per OP_GET_BLOB / OP_CHILDREN call for
    // simplicity; for chain-walk-heavy workloads, a per-stream cache
    // would be the obvious next optimisation.

    match op {

        OP_GET_BLOB => {
            let hash = recv_hash(recv).await?;
            let in_scope_flag;
            let data = {
                let guard = snap_arc.lock().unwrap();
                let scope_ok = guard.as_ref()
                    .map(|snap| blob_in_scope(snap.as_ref(), &verified, &hash))
                    .unwrap_or(false);
                in_scope_flag = scope_ok;
                guard.as_ref().and_then(|snap| {
                    if !scope_ok { return None; }
                    snap.get_blob(&hash)
                })
            };
            match data {
                Some(data) => {
                    debug!(hash = %hex::encode(&hash[..4]), bytes = data.len(), "OP_GET_BLOB served");
                    send_u64_be(send, data.len() as u64).await?;
                    send.write_all(&data).await.map_err(|e| anyhow::anyhow!("send: {e}"))?;
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
            let mut parent_in_scope = true;
            let mut total_chunks = 0usize;
            let children: Vec<RawHash> = {
                let guard = snap_arc.lock().unwrap();
                match guard.as_ref() {
                    None => Vec::new(),
                    Some(snap) => {
                        // Compute the reachable set once for this op
                        // and check membership against it for every
                        // candidate — avoids the previous O(K×N) BFS
                        // re-walk per child.
                        let reachable = reachable_set_for(
                            snap.as_ref(),
                            &verified,
                        );
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
                            parent_in_scope = false;
                            Vec::new()
                        } else {
                            match snap.get_blob(&parent_hash) {
                                None => Vec::new(),
                                Some(parent_data) => {
                                    let mut result = Vec::new();
                                    for chunk in parent_data.chunks(32) {
                                        if chunk.len() == 32 {
                                            total_chunks += 1;
                                            let mut candidate = [0u8; 32];
                                            candidate.copy_from_slice(chunk);
                                            if in_scope(&candidate) {
                                                result.push(candidate);
                                            }
                                        }
                                    }
                                    result
                                }
                            }
                        }
                    }
                }
            };
            if !parent_in_scope {
                warn!(parent = %hex::encode(&parent_hash[..4]), "OP_CHILDREN denied: parent out of scope");
            } else {
                debug!(
                    parent = %hex::encode(&parent_hash[..4]),
                    candidates = total_chunks,
                    in_scope = children.len(),
                    "OP_CHILDREN served"
                );
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

/// Build the reachable set for the given verified cap once. Returns
/// `None` if the cap is unrestricted (i.e. every present blob is in
/// scope — caller short-circuits to `snap.has_blob` checks).
/// Returns `Some(set)` for branch-restricted caps; the BFS walks
/// from each allowed branch's head following 32-byte child chunks
/// in blob bytes, just like the OP_CHILDREN handler does.
///
/// This is a per-op O(reachable subgraph) computation. Previously
/// `blob_in_scope` re-did this BFS for every blob a single
/// `OP_CHILDREN` response had to test (parent + every candidate
/// child) — worst case `O(K × N)` for K children and N reachable
/// blobs. Computing the set once amortises the BFS across the
/// whole response.
fn reachable_set_for(
    snap: &dyn AnySnapshot,
    verified: &triblespace_core::repo::capability::VerifiedCapability,
) -> Option<HashSet<RawHash>> {
    if verified.granted_branches().is_none() {
        // Unrestricted cap: every blob present in the snapshot is in
        // scope. The cap may still lack read permission entirely; in
        // that case `grants_read()` is false and the branch-level
        // gate would have filtered every head — caller cross-checks
        // via `verified.grants_read()` before consulting this set.
        return None;
    }

    let mut frontier: Vec<RawHash> = snap
        .list_branches()
        .iter()
        .filter_map(|(bid, head)| {
            triblespace_core::id::Id::new(*bid)
                .filter(|id| verified.grants_read_on(id))
                .map(|_| *head)
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
/// children references) from at least one branch head the `verified` cap
/// grants read access on. Unrestricted caps short-circuit to `true` for
/// every hash present in the snapshot.
///
/// Convenience wrapper over [`reachable_set_for`] for callers that only
/// need to test a single hash. Multi-hash callers (e.g. `OP_CHILDREN`)
/// should compute the set once and check membership directly to avoid
/// recomputing the BFS per candidate.
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

