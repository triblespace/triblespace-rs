//! Static address lookup for known peers.
//!
//! Provides [`StaticAddressLookup`] — an [`AddressLookup`] implementation
//! seeded with a fixed map of `EndpointId → EndpointAddr` at endpoint
//! construction time. Use it when callers pass `EndpointTicket`s (or
//! pre-resolved `EndpointAddr`s) through `--peers` and want the gossip
//! bootstrap path to skip iroh's pkarr/DNS discovery for those known
//! peers.
//!
//! Without this, only the `pile net pull` path uses the embedded
//! addresses (because it calls `Endpoint::connect(EndpointAddr, ALPN)`
//! directly); the gossip bootstrap path goes through
//! `Endpoint::connect(EndpointId, ALPN)`, which falls through to
//! whatever `AddressLookup` services are registered on the endpoint.
//! Adding this provider closes that gap.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Item};
use iroh_base::{EndpointAddr, EndpointId};
use n0_future::boxed::BoxStream;
use n0_future::stream::StreamExt;

/// Iroh `AddressLookup` provider seeded with a fixed map of
/// `EndpointId → EndpointAddr`. Use when the dialer already knows the
/// peer's relay URL / direct addresses and doesn't want to go through
/// iroh's discovery layer to re-learn them.
///
/// `publish` is a no-op — we don't propagate addresses anywhere
/// (that's what the DNS / pkarr providers do). `resolve` looks the
/// id up in the map and yields one [`Item`] if present, terminating
/// the stream immediately; or an empty stream if unknown, so other
/// registered providers (DNS, pkarr, mDNS) get a chance.
#[derive(Debug, Clone)]
pub struct StaticAddressLookup {
    map: Arc<HashMap<EndpointId, EndpointAddr>>,
}

impl StaticAddressLookup {
    /// Constructs a [`StaticAddressLookup`] from an iterator of known
    /// peer addresses. The endpoint id field of each [`EndpointAddr`]
    /// is used as the lookup key.
    pub fn new(addrs: impl IntoIterator<Item = EndpointAddr>) -> Self {
        let map = addrs
            .into_iter()
            .map(|addr| (addr.id, addr))
            .collect::<HashMap<_, _>>();
        Self { map: Arc::new(map) }
    }
}

const PROVENANCE: &str = "triblespace-net/static";

impl AddressLookup for StaticAddressLookup {
    fn publish(&self, _data: &EndpointData) {}

    fn resolve(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<BoxStream<Result<Item, iroh::address_lookup::Error>>> {
        match self.map.get(&endpoint_id) {
            Some(addr) => {
                let data = EndpointData::from_iter(addr.addrs.iter().cloned());
                let info = EndpointInfo::from_parts(endpoint_id, data);
                let item = Item::new(info, PROVENANCE, None);
                Some(n0_future::stream::once(Ok(item)).boxed())
            }
            None => Some(n0_future::stream::empty().boxed()),
        }
    }
}
