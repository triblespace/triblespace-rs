//! Channel types bridging the async network thread and the sync store layer.
//!
//! `NetCommand`: outgoing effects sent from a [`Peer`](crate::peer::Peer)
//! into the network thread. Most are fire-and-forget (announce, gossip,
//! track). The "Rpc" variants carry a `Sender` for the network thread to
//! reply on, which the calling Peer method blocks on.
//! `NetEvent`: incoming data sent back from the network thread to be
//! applied into the wrapped store.

use std::sync::mpsc::Sender;

use triblespace_core::id::Id;

use crate::protocol::{RawBranchId, RawHash};

/// A 32-byte public key identifying a publisher.
pub type PublisherKey = [u8; 32];

/// Commands sent to the network thread.
pub enum NetCommand {
    /// Announce a blob hash to the DHT (fire-and-forget).
    Announce(RawHash),
    /// Gossip a HEAD change for a branch (fire-and-forget).
    Gossip { branch: RawBranchId, head: RawHash },
    /// Start tracking a remote branch: recursively fetch the blobs
    /// reachable from its head and materialize a tracking branch
    /// (fire-and-forget — results arrive via `NetEvent`s).
    ///
    /// `peer` is an `EndpointAddr`, not just `EndpointId`, so a
    /// relay URL and/or direct socket addresses can be carried
    /// through to iroh's connect path without requiring
    /// discovery — crucial for environments where pkarr publish
    /// / relay probes are blocked.
    Track { peer: iroh_base::EndpointAddr, branch: RawBranchId },

    /// RPC: list a remote peer's branches. One protocol round trip.
    /// Replies with the (branch_id, branch_metadata_blob_hash) pairs.
    ListBranches {
        peer: iroh_base::EndpointAddr,
        reply: Sender<anyhow::Result<Vec<(Id, RawHash)>>>,
    },
    /// RPC: query a remote peer for its current head of one branch.
    /// One protocol round trip.
    HeadOfRemote {
        peer: iroh_base::EndpointAddr,
        branch: RawBranchId,
        reply: Sender<anyhow::Result<Option<RawHash>>>,
    },
    /// RPC: fetch a single blob by hash from a remote peer. One protocol
    /// round trip. Replies with the blob bytes (or `None` if the remote
    /// doesn't have it). The Peer wrapper method is responsible for
    /// putting the bytes into the local store.
    Fetch {
        peer: iroh_base::EndpointAddr,
        hash: RawHash,
        reply: Sender<anyhow::Result<Option<Vec<u8>>>>,
    },
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
