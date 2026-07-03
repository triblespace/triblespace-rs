//! The transport seam — everything the sync protocol needs from a
//! network, as traits.
//!
//! This module exists so the *entire* protocol stack above it — the
//! host loop, OP_AUTH / OP_CHILDREN / OP_GET_BLOB / OP_DELIVER_CAP
//! handlers, `fetch_reachable`'s two-phase walk, cap verification,
//! gossip-driven head tracking, the renewal daemon's redispatch — can
//! run unmodified against either:
//!
//! - [`crate::transport::iroh`]: the production adapter (iroh QUIC,
//!   iroh-gossip PlumTree, the embedded Kademlia DHT node), or
//! - a deterministic in-memory simulator (discrete-event router with
//!   seeded delays, drops, partitions, and crashes) for
//!   FoundationDB/TigerBeetle-style simulation testing.
//!
//! Design rule: the seam carries *capabilities*, not protocol. Anything
//! that decides what bytes mean (ALPN dispatch targets, frame layouts,
//! auth semantics, scope checks) lives above; anything that decides how
//! bytes move (QUIC, relays, NAT traversal, mesh membership, DHT
//! routing) lives below. The week-of-2026-06-04 bug hunt found every
//! protocol bug *above* this line (snapshot/gossip ordering,
//! dialer-equals-issuer, sig-vs-cap handle confusion), which is why the
//! host loop must run inside the simulator rather than being mocked
//! out at the `NetCommand`/`NetEvent` channel boundary.
//!
//! Stream IO is plain `tokio::io::{AsyncRead, AsyncWrite}` — iroh's
//! QUIC streams already implement both, and an in-memory duplex pipe
//! trivially does. `SendStream::finish()` maps to
//! `AsyncWriteExt::shutdown()`.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// A 32-byte node identity — the ed25519 pubkey bytes that double as
/// the iroh endpoint id in production and as the node address in the
/// simulator. Same value as `crate::channel::PublisherKey`.
pub type PeerId = [u8; 32];

/// Application-layer protocol identifier for a connection. Both of the
/// protocol's ALPNs are `'static` consts ([`crate::protocol::PILE_SYNC_ALPN`],
/// [`crate::handshake::AUTH_HANDSHAKE_ALPN`]), so a borrowed static slice
/// suffices and keeps dispatch alloc-free.
pub type Alpn = &'static [u8];

/// A bidirectional connection to one remote peer on one ALPN.
///
/// Mirrors the slice of iroh's `Connection` the protocol actually
/// uses: open/accept bidirectional byte streams, learn the remote's
/// TLS-verified identity, close with a code. Clone is shallow
/// (`Arc`-like) — the pool and concurrent stream users share one
/// connection.
pub trait Conn: Clone + Send + Sync + 'static {
    type SendHalf: AsyncWrite + Unpin + Send + 'static;
    type RecvHalf: AsyncRead + Unpin + Send + 'static;

    /// The remote peer's verified identity. In production this is
    /// iroh's TLS-level `remote_id` — the value the
    /// dialer-equals-issuer check and OP_AUTH subject binding trust.
    /// The simulator forges nothing: it returns the actual id of the
    /// node that dialed, so identity-dependent protocol logic is
    /// exercised honestly.
    fn remote_id(&self) -> PeerId;

    /// Open an outgoing bidirectional stream.
    fn open_bi(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<(Self::SendHalf, Self::RecvHalf)>> + Send;

    /// Accept the next incoming bidirectional stream on this
    /// connection, or `None` when the connection is closed.
    fn accept_bi(
        &self,
    ) -> impl std::future::Future<Output = Option<(Self::SendHalf, Self::RecvHalf)>> + Send;

    /// Close the connection (best-effort, fire-and-forget).
    fn close(&self, code: u32, reason: &[u8]);
}

/// Events surfaced by the gossip mesh for the team topic.
#[derive(Debug, Clone)]
pub enum GossipEvent {
    /// A broadcast frame arrived. `delivered_from` is the mesh
    /// neighbor that relayed it (NOT necessarily the original
    /// publisher — the publisher's id rides inside the frame).
    Received {
        bytes: Vec<u8>,
        delivered_from: PeerId,
    },
    /// A peer joined our active mesh view.
    NeighborUp(PeerId),
    /// A peer left our active mesh view.
    NeighborDown(PeerId),
}

/// The broadcast half of the gossip topic.
pub trait GossipSink: Clone + Send + Sync + 'static {
    /// Flood a frame to the topic. Delivery is best-effort epidemic
    /// broadcast; duplicates are deduped by the mesh layer.
    fn broadcast(
        &self,
        frame: Vec<u8>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

/// The network capabilities the protocol consumes. One instance per
/// node; `Clone` is shallow.
///
/// Deliberately *not* part of the trait: endpoint construction,
/// relay/discovery configuration, gossip topic join, and protocol
/// (ALPN) registration. Those are adapter-construction concerns —
/// see [`Harness`] for the bundle a constructor hands to the host
/// loop.
pub trait Transport: Clone + Send + Sync + 'static {
    type Conn: Conn;
    type Gossip: GossipSink;

    /// Our own identity (= the pubkey of the signing key the node
    /// runs as).
    fn local_id(&self) -> PeerId;

    /// Dial `peer` on `alpn`. Address resolution is the transport's
    /// problem (iroh: relay + pkarr + mDNS + DHT lookup; sim: direct
    /// table lookup, subject to simulated partitions).
    fn dial(
        &self,
        peer: PeerId,
        alpn: Alpn,
    ) -> impl std::future::Future<Output = anyhow::Result<Self::Conn>> + Send;

    /// Announce to the content-discovery layer that we hold `hash`.
    fn dht_announce(
        &self,
        hash: [u8; 32],
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Ask the content-discovery layer who holds `hash`. Order is
    /// meaningful to callers only as a candidate list; may include
    /// ourselves (callers filter).
    fn dht_providers(
        &self,
        hash: [u8; 32],
    ) -> impl std::future::Future<Output = Vec<PeerId>> + Send;
}

/// An accepted inbound connection, tagged with the ALPN it arrived on.
pub struct Incoming<C> {
    pub alpn: Alpn,
    pub conn: C,
}

/// Everything a transport constructor hands the host loop: the
/// dial/discovery capabilities, the inbound-connection stream, and
/// (when the node participates in gossip) the topic's broadcast half
/// plus its event stream.
///
/// Both halves of every channel are owned here rather than living in
/// trait methods so that adapter construction — which for iroh has to
/// register ALPN handlers and join the gossip topic *before* the
/// router spawns — happens in one place, and the host loop receives a
/// ready-to-run bundle.
pub struct Harness<T: Transport> {
    pub transport: T,
    pub incoming: mpsc::Receiver<Incoming<T::Conn>>,
    pub gossip: Option<(T::Gossip, mpsc::Receiver<GossipEvent>)>,
}

pub mod iroh;

#[cfg(feature = "sim")]
pub mod sim;
