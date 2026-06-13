//! Channel types bridging the async network thread and the sync store layer.
//!
//! `NetCommand`: outgoing effects sent from a [`Peer`](crate::peer::Peer)
//! into the network thread. All fire-and-forget — there are no RPC
//! variants because branch-state discovery is gossip-driven, not
//! peer-targeted.
//! `NetEvent`: incoming data sent back from the network thread to be
//! applied into the wrapped store.
//!
//! Byte payloads use [`anybytes::Bytes`] rather than `Vec<u8>`:
//! Bytes is Arc-refcounted, so cloning across the channel boundary
//! is a refcount bump instead of a full byte-copy. The same payload
//! can flow into multiple onward sinks (wire write + local pin)
//! without re-materialising the buffer.

use anybytes::Bytes;

use crate::protocol::{RawPinId, RawHash};

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
    Gossip { branch: RawPinId, head: RawHash },
    /// Dispatch a freshly-signed cap+sig pair to `subject` via the
    /// auth-handshake ALPN. Used by the renewal daemon (push-based
    /// renewal) and by the `team approve` subcommand (response to a
    /// pending request). The network thread opens a connection to
    /// the subject's pubkey, sends `OP_DELIVER_CAP`, and closes.
    ///
    /// Delivery is best-effort fire-and-forget at this layer.
    /// Confirmation happens later, when the subject actually
    /// authenticates against our pile-sync ALPN presenting the
    /// delivered cap — see `NetEvent::CapDeliveryConfirmed`. The
    /// renewal daemon redispatches entries that haven't been
    /// confirmed yet (per-entry cooldown to avoid hammering an
    /// unreachable peer).
    DeliverCap {
        subject: PublisherKey,
        cap_bytes: Bytes,
        sig_bytes: Bytes,
    },
    /// Swarm-addressed on-demand blob fetch — the lazy-replication
    /// read-miss path. The caller (a `PeerReader` whose local stores
    /// missed) hands a content hash and a reply channel; the host
    /// resolves providers via the DHT (no peer is named — this is
    /// NOT the pre-May peer-targeted `Fetch`, it is `get(hash)`
    /// asking "whoever holds it"), fetches, verifies `blake3(bytes)
    /// == hash`, and sends the bytes (or `None` if unavailable after
    /// the deadline) back on `reply`.
    ///
    /// This is the one request-reply command — `NetCommand` is
    /// otherwise fire-and-forget — and it reuses the exact shape the
    /// removed `NetCommand::Fetch` had (std-mpsc reply over the same
    /// sender/host split), so the mechanism is proven. The reply
    /// being `None` (rather than the channel dropping) is the clean
    /// "Unavailable" the read API surfaces; a dropped channel (host
    /// gone) the caller also treats as Unavailable, never a hang.
    FetchBlob {
        hash: RawHash,
        reply: std::sync::mpsc::Sender<Option<Vec<u8>>>,
    },
}

/// Events received from the network thread.
#[derive(Debug)]
pub enum NetEvent {
    /// A blob was fetched from the network.
    Blob(Bytes),
    /// A remote branch HEAD was learned (via gossip or fetch).
    /// Includes the publisher's public key for provenance.
    Head { branch: RawPinId, head: RawHash, publisher: PublisherKey },
    /// A peer asked us to issue them a capability. The partial cap
    /// blob carries the subject they're requesting for (must match
    /// `requester` — verified at connection time via iroh's TLS),
    /// the scope they're asking for, and their preferred expiry
    /// interval. The local renewal-policy branch decides whether
    /// to auto-approve, queue for human review, or reject.
    CapRequest {
        requester: PublisherKey,
        partial_cap_bytes: Bytes,
    },
    /// A peer issued us a capability — either in response to a prior
    /// `CapRequest` we made, or as an unsolicited renewal push. The
    /// cap+sig bytes are content-verified before pinning into the
    /// local team-cap branch.
    CapDelivered {
        issuer: PublisherKey,
        cap_bytes: Bytes,
        sig_bytes: Bytes,
    },
    /// `subject` successfully authenticated against our pile-sync
    /// `OP_AUTH` stream by presenting signature handle `sig_handle`.
    /// This is the unambiguous "the subject has the cap and uses
    /// it" signal — the wire-level STATUS_OK on `OP_DELIVER_CAP`
    /// only tells us the bytes landed; auth tells us the subject
    /// can both load AND verify the chain. The Peer side uses this
    /// to mark the matching renewal-policy entry as delivered so
    /// the daemon's next tick skips it from the redispatch set.
    ///
    /// Field is the *signature* handle, not the cap blob handle —
    /// OP_AUTH wires the sig blob since that's the credential the
    /// dialer needs to prove possession of. Match against
    /// `PolicyEntry::latest_sig` (not `latest_cap`) when looking up
    /// the corresponding renewal-policy entry.
    CapDeliveryConfirmed {
        subject: PublisherKey,
        sig_handle: RawHash,
    },
}