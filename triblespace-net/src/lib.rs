//! Distributed sync for triblespace.
//!
//! The main type is [`Peer<S>`](peer::Peer): a store wrapper that owns an
//! iroh network thread internally and exposes the standard storage traits
//! (`BlobStore + BlobStorePut + BranchStore`). Reads auto-drain incoming
//! gossip; writes auto-publish to the gossip topic and DHT. The user thinks
//! of it as "my store, but networked."
//!
//! All store traits stay sync. Async is jailed inside the network thread.

mod address_lookup;
mod channel;
pub mod dht;
mod host;
pub mod peer;
pub mod protocol;
pub mod identity;
pub mod tracking;

/// Normalises an [`iroh_base::EndpointAddr`] by stripping trailing
/// FQDN dots from any relay URLs it carries. See the implementation
/// in `host` for the full rationale; the short version is that iroh
/// 0.98's default relay hostnames are FQDN-absolute, the dot
/// propagates through `Endpoint::addr()` even when our outbound
/// RelayMap is dot-free, and the dotted URL trips WAFs on receiving
/// peers. Apply at every channel boundary that emits or consumes an
/// `EndpointAddr` (ticket encode/decode, etc.).
pub use crate::host::dot_stripped_endpoint_addr;

