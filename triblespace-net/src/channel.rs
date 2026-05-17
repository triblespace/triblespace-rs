//! Channel types bridging the async network thread and the sync store layer.
//!
//! `NetCommand`: outgoing effects sent from a [`Peer`](crate::peer::Peer)
//! into the network thread. All fire-and-forget — there are no RPC
//! variants because branch-state discovery is gossip-driven, not
//! peer-targeted.
//! `NetEvent`: incoming data sent back from the network thread to be
//! applied into the wrapped store.

use crate::protocol::{RawBranchId, RawHash};

/// A 32-byte public key identifying a publisher.
pub type PublisherKey = [u8; 32];

/// Commands sent to the network thread.
///
/// The surface is minimal by design — branch-state discovery is
/// gossip-driven (HEAD updates flood the team topic; the network
/// thread autonomously walks reachable closures via the DHT-routed
/// `OP_GET_BLOB` + `OP_CHILDREN` path). No peer-targeted RPCs.
pub enum NetCommand {
    /// Announce a blob hash to the DHT (fire-and-forget). Local
    /// puts trigger this; new providers improve the swarm's
    /// content-distribution fan-out.
    Announce(RawHash),
    /// Gossip a HEAD change for a branch (fire-and-forget). Local
    /// branch updates trigger this; subscribers on the team topic
    /// receive the flood message and walk the closure to catch up.
    Gossip { branch: RawBranchId, head: RawHash },
}

/// Events received from the network thread.
#[derive(Debug)]
pub enum NetEvent {
    /// A blob was fetched from the network.
    Blob(Vec<u8>),
    /// A remote branch HEAD was learned (via gossip or fetch).
    /// Includes the publisher's public key for provenance.
    Head { branch: RawBranchId, head: RawHash, publisher: PublisherKey },
}
