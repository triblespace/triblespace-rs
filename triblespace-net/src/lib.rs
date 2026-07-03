//! Distributed sync for triblespace.
//!
//! The main type is [`Peer<S>`](peer::Peer): a store wrapper that owns an
//! iroh network thread internally and exposes the standard storage traits
//! (`BlobStore + BlobStorePut + PinStore`). Reads auto-drain incoming
//! gossip; writes auto-publish to the gossip topic and DHT. The user thinks
//! of it as "my store, but networked."
//!
//! All store traits stay sync. Async is jailed inside the network thread.

mod channel;

/// Base backoff shared by the crate's retry loops (failed head-walk
/// retries in [`host`], failed want-fetch retries in
/// [`reconcile::Reconciler`]); doubles per attempt up to
/// [`RETRY_BACKOFF_CAP`]. Values chosen so a transient fault (peer
/// restarting, partition healing) is retried promptly while a
/// persistently-dead source costs at most one attempt per cap period.
pub(crate) const RETRY_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(1);
/// Upper bound the exponential retry backoff saturates at.
pub(crate) const RETRY_BACKOFF_CAP: std::time::Duration = std::time::Duration::from_secs(60);
pub mod clock;
pub mod dht;
pub mod handshake;
pub mod host;
pub mod peer;
pub mod policy;
pub mod protocol;
pub mod reconcile;
pub mod identity;
pub mod tracking;
pub mod transport;

