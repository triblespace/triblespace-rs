//! Distributed sync for triblespace.
//!
//! The main type is [`Peer<S>`](peer::Peer): a store wrapper that owns an
//! iroh network thread internally and exposes the standard storage traits
//! (`BlobStore + BlobStorePut + BranchStore`). Reads auto-drain incoming
//! gossip; writes auto-publish to the gossip topic and DHT. The user thinks
//! of it as "my store, but networked."
//!
//! All store traits stay sync. Async is jailed inside the network thread.

mod channel;
pub mod dht;
mod host;
pub mod peer;
pub mod protocol;
pub mod identity;
pub mod tracking;

