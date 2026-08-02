//! Channel types bridging the async network thread and the sync store layer.
//!
//! `NetCommand`: outgoing effects sent from a [`Peer`](crate::peer::Peer)
//! into the network thread. All fire-and-forget.
//! `NetEvent`: incoming data sent back from the network thread to be
//! applied into the wrapped store.
//!
//! Byte payloads use [`anybytes::Bytes`] rather than `Vec<u8>`:
//! Bytes is Arc-refcounted, so cloning across the channel boundary
//! is a refcount bump instead of a full byte-copy. The same payload
//! can flow into multiple onward sinks (wire write + local pin)
//! without re-materialising the buffer.

use anybytes::Bytes;

use crate::protocol::RawHash;

/// A 32-byte endpoint/publisher hint carried by legacy transport.
pub type PublisherKey = [u8; 32];

/// Commands sent to the network thread.
///
/// The surface is minimal by design.
pub enum NetCommand {
    /// Announce a blob hash to the DHT (fire-and-forget). Local
    /// puts trigger this; new providers improve the swarm's
    /// content-distribution fan-out.
    Announce(RawHash),
    /// Replace the capability presented on every future outbound pile-sync
    /// connection. The host evicts all pooled connections when applying this
    /// command, so no connection authenticated with the predecessor remains
    /// reusable after credential activation.
    UpdateSelfCap(RawHash),
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
    // The swarm-addressed read-miss fetch is no longer a command: it
    // runs inline via `NetSender::fetch_blob` / `host::NetCapability`,
    // so there is no `FetchBlob` round-trip through this loop.
}

/// Events received from the network thread.
#[derive(Debug)]
pub enum NetEvent {
    /// A peer asked us to issue them a capability. The partial cap
    /// blob carries the subject they're requesting for (must match
    /// `requester` — verified at connection time via iroh's TLS),
    /// the scope they're asking for, and their preferred expiry
    /// interval. The local renewal-policy branch decides whether
    /// to auto-approve, queue for human review, or reject.
    CapRequest {
        requester: PublisherKey,
        partial_cap_bytes: Bytes,
        /// Admission token for the bounded pre-policy request queue. Keeping
        /// the permit in the event means capacity is released automatically
        /// when the Peer consumes or drops this request.
        admission: tokio::sync::OwnedSemaphorePermit,
        /// Completed by the synchronous Peer only for a known policy outcome:
        /// `true` means the request was durably recorded and permits
        /// `STATUS_OK`; `false` means policy definitely refused it and permits
        /// `STATUS_REJECTED`. Persistence failure drops this sender so the host
        /// returns `STATUS_INDETERMINATE`, because a failed append may still
        /// have taken effect. Peer shutdown and request timeout likewise never
        /// manufacture a definitive negative receipt.
        completion: tokio::sync::oneshot::Sender<bool>,
    },
    /// A peer issued us a capability — either in response to a prior
    /// `CapRequest` we made, or as an unsolicited renewal push. The
    /// cap+sig bytes are content-verified before pinning into the
    /// local team-cap branch.
    CapDelivered {
        issuer: PublisherKey,
        cap_bytes: Bytes,
        sig_bytes: Bytes,
        /// Every non-leaf member of the complete bounded proof closure used
        /// during verification, including members already present in the
        /// host snapshot. The complete bundle is one event so a snapshot
        /// rotation cannot separate the active leaf from its proof.
        proof_blobs: Vec<Bytes>,
        /// Inclusive upper bound of the verified authority's lifetime: the
        /// earliest deadline in the entire delegation chain, not merely the
        /// leaf capability's declared expiry.
        authority_expires_at: hifitime::Epoch,
        /// Bounds verified deliveries waiting for synchronous persistence.
        admission: tokio::sync::OwnedSemaphorePermit,
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
        /// Best-effort notification queue admission. Dropping a confirmation
        /// is safe: the issuer simply keeps the renewal entry eligible for a
        /// later redispatch/auth confirmation.
        admission: tokio::sync::OwnedSemaphorePermit,
    },
}
