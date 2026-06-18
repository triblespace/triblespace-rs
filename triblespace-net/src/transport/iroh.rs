//! Production [`Transport`] adapter: iroh QUIC + iroh-gossip + the
//! embedded Kademlia DHT node.
//!
//! Everything iroh-specific that used to live inline in the host
//! loop's startup — endpoint building (relay map, CA roots, mDNS +
//! pkarr + mDNS address lookup), protocol-handler registration, gossip
//! topic join, DHT node spawn — happens in [`bind`], which returns the
//! transport-agnostic [`Harness`] the host loop runs against.

use std::sync::Arc;

use iroh_base::EndpointId;
use tokio::sync::mpsc;
use tracing::warn;

use super::{Alpn, Conn, GossipEvent, GossipSink, Harness, Incoming, PeerId, Transport};
use crate::host::PeerConfig;

/// Capacity for the inbound-connection and gossip-event channels.
/// Backpressure past this point slows the QUIC accept loop, which is
/// the desired failure mode (better than unbounded buffering).
const CHANNEL_CAP: usize = 64;

/// The two protocol ALPNs forwarded into the host loop. The DHT and
/// gossip ALPNs are *not* forwarded — their handlers are registered
/// directly with the router here and never surface above the seam.
const FORWARDED_ALPNS: [Alpn; 2] = [
    crate::protocol::PILE_SYNC_ALPN,
    crate::handshake::AUTH_HANDSHAKE_ALPN,
];

#[derive(Clone)]
pub struct IrohTransport {
    ep: iroh::Endpoint,
    dht: Option<crate::dht::api::ApiClient>,
    /// Keeps the router (and through it the registered protocol
    /// handlers + gossip + DHT rpc node) alive for the transport's
    /// lifetime. The host loop never touches these; they exist below
    /// the seam.
    _alive: Arc<Anchors>,
}

/// Owner of everything that must not drop while the node runs.
struct Anchors {
    _router: iroh::protocol::Router,
    _dht_rpc: Option<crate::dht::rpc::RpcClient>,
}

#[derive(Clone)]
pub struct IrohConn(pub iroh::endpoint::Connection);

impl Conn for IrohConn {
    type SendHalf = iroh::endpoint::SendStream;
    type RecvHalf = iroh::endpoint::RecvStream;

    fn remote_id(&self) -> PeerId {
        *self.0.remote_id().as_bytes()
    }

    async fn open_bi(&self) -> anyhow::Result<(Self::SendHalf, Self::RecvHalf)> {
        self.0
            .open_bi()
            .await
            .map_err(|e| anyhow::anyhow!("open_bi: {e}"))
    }

    async fn accept_bi(&self) -> Option<(Self::SendHalf, Self::RecvHalf)> {
        self.0.accept_bi().await.ok()
    }

    fn close(&self, code: u32, reason: &[u8]) {
        self.0.close(code.into(), reason);
    }
}

#[derive(Clone)]
pub struct IrohGossip(iroh_gossip::api::GossipSender);

impl GossipSink for IrohGossip {
    async fn broadcast(&self, frame: Vec<u8>) -> anyhow::Result<()> {
        self.0
            .broadcast(frame.into())
            .await
            .map_err(|e| anyhow::anyhow!("gossip broadcast: {e}"))
    }
}

impl Transport for IrohTransport {
    type Conn = IrohConn;
    type Gossip = IrohGossip;

    fn local_id(&self) -> PeerId {
        *self.ep.id().as_bytes()
    }

    async fn dial(&self, peer: PeerId, alpn: Alpn) -> anyhow::Result<Self::Conn> {
        let id = EndpointId::from_bytes(&peer)
            .map_err(|e| anyhow::anyhow!("peer id: {e}"))?;
        let conn = self
            .ep
            .connect(id, alpn)
            .await
            .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
        Ok(IrohConn(conn))
    }

    async fn dht_announce(&self, hash: [u8; 32]) {
        if let Some(api) = &self.dht {
            let blake3_hash = blake3::Hash::from_bytes(hash);
            let _ = api.announce_provider(blake3_hash, self.ep.id()).await;
        }
    }

    async fn dht_providers(&self, hash: [u8; 32]) -> Vec<PeerId> {
        let Some(api) = &self.dht else {
            return Vec::new();
        };
        let blake3_hash = blake3::Hash::from_bytes(hash);
        match api.find_providers(blake3_hash).await {
            Ok(ids) => ids.into_iter().map(|id| *id.as_bytes()).collect(),
            Err(_) => Vec::new(),
        }
    }
}

/// Thin `ProtocolHandler` that forwards accepted connections (tagged
/// with their ALPN) into the harness channel. The host loop owns the
/// conversation from there.
///
/// The handler returns as soon as the connection is forwarded; the
/// `Connection` is internally reference-counted, so the clone living
/// in the channel (and later in the host's per-connection task) keeps
/// it alive after the router's accept task completes.
#[derive(Clone)]
struct ForwardHandler {
    alpn: Alpn,
    tx: mpsc::Sender<Incoming<IrohConn>>,
}

impl std::fmt::Debug for ForwardHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardHandler").finish()
    }
}

impl iroh::protocol::ProtocolHandler for ForwardHandler {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let _ = self
            .tx
            .send(Incoming {
                alpn: self.alpn,
                conn: IrohConn(connection),
            })
            .await;
        Ok(())
    }
}

/// Build the production transport: bind the iroh endpoint (OS trust
/// store, dot-stripped default relays, N0 pkarr+DNS + mDNS address
/// lookup), spawn the embedded DHT node, register the protocol-
/// forwarding handlers, join the team gossip topic when configured,
/// and spawn the router.
///
/// Returns `None` if the endpoint fails to bind (already logged) —
/// the caller's net thread exits, mirroring the old inline behavior.
pub async fn bind(
    secret: iroh_base::SecretKey,
    config: &PeerConfig,
) -> Option<Harness<IrohTransport>> {
    use iroh::endpoint::presets;
    use iroh::protocol::Router;
    use iroh::Endpoint;
    use iroh_gossip::Gossip;

    // Use the OS trust store (via rustls-platform-verifier) rather
    // than the compiled-in Mozilla webpki-roots bundle. The default
    // (webpki-roots) breaks when running inside corporate-proxy /
    // sandbox environments that present a custom CA at egress: iroh's
    // relay HTTPS probes and pkarr publish/lookup over HTTPS get
    // `invalid peer certificate: UnknownIssuer`, discovery dies
    // silently, and the QUIC handshake never starts. Reading the OS
    // store at runtime lets the sandbox CA (or any admin-installed
    // root) participate. Relay hostnames get their FQDN trailing dot
    // stripped — see `dot_stripped_default_relay_map` for the WAF
    // story.
    let relay_map = crate::host::dot_stripped_default_relay_map();

    // Discovery: `presets::N0` gives pkarr publish + DNS lookup via
    // n0.computer. On top of that we add mDNS for local-network
    // discovery (zero-conf, no internet needed). pkarr-DHT address
    // discovery was removed from iroh core in 1.0; we deliberately stay
    // on the well-supported N0 + mDNS path rather than re-add it.
    let builder = Endpoint::builder(presets::N0)
        .secret_key(secret.clone())
        .ca_tls_config(iroh::tls::CaTlsConfig::system())
        .relay_mode(iroh::RelayMode::Custom(relay_map));
    let ep = match builder.bind().await {
        Ok(ep) => ep,
        Err(e) => {
            tracing::error!(error = %e, "iroh endpoint bind failed; net thread exiting");
            return None;
        }
    };
    // mDNS is best-effort — add it post-bind so a failure (e.g. no
    // multicast on the interface) degrades to N0-only rather than
    // failing the whole endpoint.
    match iroh_mdns_address_lookup::MdnsAddressLookup::builder().build(ep.id()) {
        Ok(mdns) => {
            if let Ok(al) = ep.address_lookup() {
                al.add(mdns);
            }
        }
        Err(e) => {
            warn!(error = %e, "mDNS discovery init failed; continuing without LAN discovery")
        }
    }
    ep.online().await;

    let my_id = ep.id();
    let mut router_builder = Router::builder(ep.clone());

    // DHT — always on. Peers bootstrap the routing table.
    let dht_alpn = crate::dht::rpc::ALPN;
    let pool = iroh_blobs::util::connection_pool::ConnectionPool::new(
        ep.clone(),
        dht_alpn,
        iroh_blobs::util::connection_pool::Options {
            max_connections: 64,
            idle_timeout: std::time::Duration::from_secs(30),
            connect_timeout: std::time::Duration::from_secs(10),
            on_connected: None,
        },
    );
    let iroh_pool = crate::dht::pool::IrohPool::new(ep.clone(), pool);
    let bootstrap_ids: Vec<EndpointId> = config.peers.iter().map(|addr| addr.id).collect();
    let (rpc, dht_api) = crate::dht::create_node(
        my_id,
        iroh_pool.clone(),
        bootstrap_ids.clone(),
        Default::default(),
    );
    iroh_pool.set_self_client(Some(rpc.downgrade()));
    let dht_sender = rpc.inner().as_local().expect("local sender");
    router_builder =
        router_builder.accept(dht_alpn, irpc_iroh::IrohProtocol::with_sender(dht_sender));

    // Protocol ALPNs forward into the harness channel; the host loop
    // dispatches them to the protocol handlers above the seam.
    let (inc_tx, inc_rx) = mpsc::channel::<Incoming<IrohConn>>(CHANNEL_CAP);
    for alpn in FORWARDED_ALPNS {
        router_builder = router_builder.accept(
            alpn,
            ForwardHandler {
                alpn,
                tx: inc_tx.clone(),
            },
        );
    }

    // Gossip: join the team topic (topic id = team root pubkey — one
    // mesh per team) and translate iroh-gossip events into the
    // transport-agnostic GossipEvent stream. Always `subscribe`
    // (non-blocking): the join completes in the background as peers
    // come online; `subscribe_and_join` would hang nodes that start
    // at different times.
    let mut gossip = None;
    if config.gossip {
        let g = Gossip::builder().spawn(ep.clone());
        router_builder = router_builder.accept(iroh_gossip::ALPN, g.clone());
        let topic_id = iroh_gossip::TopicId::from_bytes(config.team_root.to_bytes());
        match g.subscribe(topic_id, bootstrap_ids.clone()).await {
            Ok(topic) => {
                let (sender, receiver) = topic.split();
                let (gev_tx, gev_rx) = mpsc::channel::<GossipEvent>(CHANNEL_CAP);
                tokio::spawn(async move {
                    use futures::TryStreamExt;
                    let mut receiver = receiver;
                    while let Ok(Some(event)) = receiver.try_next().await {
                        let mapped = match event {
                            iroh_gossip::api::Event::Received(msg) => Some(GossipEvent::Received {
                                bytes: msg.content.to_vec(),
                                delivered_from: *msg.delivered_from.as_bytes(),
                            }),
                            iroh_gossip::api::Event::NeighborUp(peer) => {
                                Some(GossipEvent::NeighborUp(*peer.as_bytes()))
                            }
                            iroh_gossip::api::Event::NeighborDown(peer) => {
                                Some(GossipEvent::NeighborDown(*peer.as_bytes()))
                            }
                            _ => None,
                        };
                        if let Some(ev) = mapped {
                            if gev_tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                    }
                });
                gossip = Some((IrohGossip(sender), gev_rx));
            }
            Err(e) => {
                warn!(error = %e, "gossip subscribe failed; running without gossip");
            }
        }
    }

    let router = router_builder.spawn();

    let transport = IrohTransport {
        ep,
        dht: Some(dht_api),
        _alive: Arc::new(Anchors {
            _router: router,
            _dht_rpc: Some(rpc),
        }),
    };

    Some(Harness {
        transport,
        incoming: inc_rx,
        gossip,
    })
}
