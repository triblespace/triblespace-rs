//! # Minimal DHT for iroh
//!
//! This follows a lot of the design decisions of the bittorrent mainline DHT,
//! with two major differences:
//! - we use BLAKE3 instead of SHA1, and therefore extend the keyspace to 32
//!   bytes. 32 bytes is also a more natural fit for other purposes such as
//!   storage of data for ED25519 keys like in [bep_0044]
//! - connections are not raw UDP but iroh connections, with use of [0rtt] to make
//!   the DHT typical tiny interactions faster.
//!
//! Other than that this is a pretty straightforward [kademlia] implementation,
//! using the XOR metric and standard routing tables.
//!
//! A DHT is two things:
//!
//! ## Data storage
//!
//! A multimap of keys to values, where values are self contained pieces of data
//! that have some way to be verified and have some relationship to the key.
//!
//! Values should have some kind of expiry mechanism, but they don't need
//! provenance since they are self-contained.
//!
//! The storage part of the DHT is basically a standalone tracker, except for
//! the fact that it will reject set requests that are obviously far away
//! from the node id in terms of the DHT metric.
//!
//! Disabling the latter mechanism would allow a single DHT node to act as a
//! tracker.
//!
//! Examples of values:
//!
//! - Provider node ids for a key, where the key is interpreted as a BLAKE3 hash
//!   of some data. Expiry is a timestamp, validation is checking that the node
//!   has the data by means of a BLAKE3 probe. This is equivalent to the main
//!   purpose of mainline outlined in [bep_0005].
//!
//! - A signed message, e.g. a pkarr record, where the key is interpreted as
//!   the public key of the signer. Expiry is a timestamp, validation is
//!   checking the signature against the public key. Almost identical to [bep_0044],
//!   except that we don't have to hash the key because it fits our keyspace.
//!
//! - Self-contained immutable data, where the key is interpreted as a BLAKE3
//!   hash of the data. Expiry is a timestamp, validation is checking that the
//!   data matches the hash. This is also part of [bep_0044].
//!
//! We use [postcard] on the wire and most likely also on disk.
//!
//! ## Routing
//!
//! A way to find the n most natural locations for a given key. Routing is only
//! concerned with the key, not the value.
//!
//! DHT nodes talk to each other using a simple [rpc] protocol. RPC requests can
//! always be answered using purely local information.
//!
//! A DHT node is controlled using the [api] protocol, which contains higher
//! level operations that trigger interactions with multiple DHT nodes.
//!
//! Both protocols are implemented using the [irpc] crate. As an intro to irpc,
//! see the [irpc blog post].
//!
//! [bep_0044]: https://www.bittorrent.org/beps/bep_0044.html
//! [bep_0005]: https://www.bittorrent.org/beps/bep_0005.html
//! [kademlia]: https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
//! [postcard]: https://postcard.jamesmunns.com/
//! [0rtt]: https://www.iroh.computer/blog/0rtt-api
//! [irpc]: https://docs.rs/irpc/latest/irpc/
//! [irpc blog post]: https://www.iroh.computer/blog/irpc/
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use futures_buffered::FuturesUnordered;
use indexmap::IndexSet;
use iroh::EndpointId;
use irpc::{
    LocalSender,
    channel::{mpsc, oneshot},
};
use n0_future::{BufferedStreamExt, MaybeFuture, StreamExt, stream};
use rand::{Rng, SeedableRng, rngs::StdRng, seq::index::sample};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

pub mod rpc {
    //! RPC protocol between DHT nodes.
    //!
    //! These are low level operations that only affect the node being called.
    //! E.g. finding closest nodes for a node based on the current content of
    //! the routing table, as well as storing and retrieving values from the
    //! node itself.
    //!
    //! The protocol is defined in [`RpcProto`], which has a corresponding full
    //! message type [`RpcMessage`].
    //!
    //! The entry point is [`RpcClient`].
    use std::{
        fmt,
        num::NonZeroU64,
        ops::Deref,
        sync::{Arc, Weak},
    };

    use iroh::{Endpoint, EndpointAddr, EndpointId, PublicKey};
    
    use irpc::{
        channel::{mpsc, oneshot},
        rpc_requests,
    };
    use serde::{Deserialize, Serialize};
    use serde_big_array::BigArray;

    pub const ALPN: &[u8] = b"iroh/dht/0";

    /// Entry type for BLAKE3 content discovery.
    ///
    /// Provides a similar functionality to BEP-5 in mainline, but for BLAKE3
    /// hashes instead of SHA-1 hashes.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Blake3Provider {
        pub timestamp: u64,
        pub endpoint_id: [u8; 32],
    }

    /// Small immutable value.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Blake3Immutable {
        pub timestamp: u64, // Unix timestamp for expiry
        pub data: Vec<u8>,
    }

    /// Entry type for signed messages, e.g. pkarr records, for node discovery.
    ///
    /// Provides a similar functionality BEP-44 in mainline.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ED25519SignedMessage {
        /// Unix timestamp for expiry
        pub timestamp: u64,
        /// A 64-byte signature using Ed25519
        #[serde(with = "BigArray")]
        pub signature: [u8; 64],
        /// The signed message data. This must be <= 1024 bytes so an entire
        /// set request fits a single non-fragmented UDP packet even with QUIC
        /// overhead.
        pub data: Vec<u8>,
    }

    /// DHT value type.
    ///
    /// The order of the enum is important for serialization/deserialization
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Inline {
        Blake3Provider(Blake3Provider),
        ED25519SignedMessage(ED25519SignedMessage),
        Blake3Immutable(Blake3Immutable),
    }

    impl Inline {
        /// Returns the kind of this value.
        pub fn kind(&self) -> Kind {
            match self {
                Inline::Blake3Provider(_) => Kind::Blake3Provider,
                Inline::ED25519SignedMessage(_) => Kind::ED25519SignedMessage,
                Inline::Blake3Immutable(_) => Kind::Blake3Immutable,
            }
        }
    }

    /// DHT value kind type.
    ///
    /// Must have the same order as [`Inline`] for serialization/deserialization
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Kind {
        Blake3Provider,
        ED25519SignedMessage,
        Blake3Immutable,
    }

    /// We use a 32 byte keyspace so we can represent things like modern hashes
    /// and public keys without having to map them to a smaller keyspace.
    #[derive(Clone, Copy, Ord, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Id([u8; 32]); // 256-bit identifier

    impl From<[u8; 32]> for Id {
        fn from(bytes: [u8; 32]) -> Self {
            Id(bytes)
        }
    }

    impl Deref for Id {
        type Target = [u8; 32];

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl fmt::Debug for Id {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Id({})", hex::encode(self.0))
        }
    }

    impl fmt::Display for Id {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", hex::encode(self.0))
        }
    }

    impl From<PublicKey> for Id {
        fn from(pk: PublicKey) -> Self {
            Id(*pk.as_bytes())
        }
    }

    impl From<blake3::Hash> for Id {
        fn from(pk: blake3::Hash) -> Self {
            Id(*pk.as_bytes())
        }
    }

    impl Id {
        pub fn blake3_hash(data: &[u8]) -> Self {
            let hash = blake3::hash(data);
            Id(hash.into())
        }

        pub fn endpoint_id(id: iroh::EndpointId) -> Self {
            Id::from(*id.as_bytes())
        }
    }

    /// Set a key to a value.
    ///
    /// The storage is allowed to reject set requests if the key is far away from
    /// the node id in terms of the DHT metric, or if the value is invalid.
    ///
    /// The storage is also allowed to drop values at any time. This is not a
    /// command but more a request to store the value.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Set {
        /// The key to set the value for.
        pub key: Id,
        /// The value being set.
        pub value: Inline,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum SetResponse {
        /// The set request was successful.
        Ok,
        /// The key was too far away from the node id in terms of the DHT metric.
        ErrDistance,
        /// The value is too old.
        ErrExpired,
        /// The node does not have capacity to store the value.
        ErrFull,
        /// The value is invalid, e.g. the signature does not match the public key.
        ErrInvalid,
    }

    /// Get all values of a certain kind for a key, as a stream.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct GetAll {
        /// The key to get the values for.
        pub key: Id,
        /// The kind of value to get.
        pub kind: Kind,
        /// Optional seed for randomization of the returned stream of values.
        /// If this is not provided, items will be returned in an unspecified order.
        pub seed: Option<NonZeroU64>,
        /// Number of values to return, if specified. If not specified, all values
        /// of the specified kind for the key will be returned until the receiver
        /// stops or the stream ends.
        pub n: Option<NonZeroU64>,
    }

    /// A request to query the routing table for the most natural locations
    #[derive(Debug, Serialize, Deserialize)]
    pub struct FindNode {
        /// The key to find the most natural locations (nodes) for.
        pub id: Id,
        /// The requester wants to be included in the routing table.
        ///
        /// For the irpc memory or quinn transport, you have to just believe this value.
        /// For the irpc iroh transport, this must be the node ID of the requester.
        pub requester: Option<EndpointId>,
    }

    /// Protocol for rpc communication.
    ///
    /// Note: you might wonder why there is no ping message. To do a basic
    /// liveness check, just do a FindNode with a random ID. It should be just
    /// as cheap in terms of roundtrips, and already gives you some useful
    /// information.
    #[rpc_requests(message = RpcMessage)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum RpcProto {
        /// Set a key to a value.
        #[rpc(tx = oneshot::Sender<SetResponse>)]
        Set(Set),
        /// Get all values of a certain kind for a key, as a stream of values.
        #[rpc(tx = mpsc::Sender<Inline>)]
        GetAll(GetAll),
        /// A request to query the routing table for the most natural locations
        #[rpc(tx = oneshot::Sender<Vec<EndpointAddr>>)]
        FindNode(FindNode),
    }

    #[derive(Debug, Clone)]
    pub struct RpcClient(pub(crate) Arc<irpc::Client<RpcProto>>);

    #[derive(Debug, Clone)]
    pub struct WeakRpcClient(pub(crate) Weak<irpc::Client<RpcProto>>);

    impl WeakRpcClient {
        pub fn upgrade(&self) -> Option<RpcClient> {
            self.0.upgrade().map(RpcClient)
        }
    }

    impl RpcClient {
        /// Get the inner client for use with irpc_iroh::IrohProtocol.
        pub fn inner(&self) -> &Arc<irpc::Client<RpcProto>> {
            &self.0
        }

        pub fn remote(endpoint: Endpoint, id: Id) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let id = iroh::EndpointId::from_bytes(&id)?;
            let client = irpc_iroh::client(endpoint, id, ALPN);
            Ok(Self::new(client))
        }

        pub fn new(client: irpc::Client<RpcProto>) -> Self {
            Self(Arc::new(client))
        }

        pub async fn set(&self, key: Id, value: Inline) -> irpc::Result<SetResponse> {
            self.0.rpc(Set { key, value }).await
        }

        pub async fn get_all(
            &self,
            key: Id,
            kind: Kind,
            seed: Option<NonZeroU64>,
            n: Option<NonZeroU64>,
        ) -> irpc::Result<irpc::channel::mpsc::Receiver<Inline>> {
            self.0
                .server_streaming(GetAll { key, kind, seed, n }, 32)
                .await
        }

        pub async fn find_node(
            &self,
            id: Id,
            requester: Option<EndpointId>,
        ) -> irpc::Result<Vec<EndpointAddr>> {
            self.0.rpc(FindNode { id, requester }).await
        }

        pub fn downgrade(&self) -> WeakRpcClient {
            WeakRpcClient(Arc::downgrade(&self.0))
        }
    }
}

pub mod api {
    //! RPC protocol for an user to talk to a DHT node.
    //!
    //! These are operations that affect the entire network, such as storing or retrieving a value.
    //!
    //! The protocol is defined in [`ApiProto`], which has a corresponding full
    //! message type [`ApiMessage`].
    //!
    //! The entry point is [`ApiClient`].
    use std::{
        collections::BTreeMap,
        num::NonZeroU64,
        sync::{Arc, Weak},
        time::Duration,
    };

    use iroh::EndpointId;
    use irpc::{
        channel::{mpsc, none::NoSender, oneshot},
        rpc_requests,
    };
    use serde::{Deserialize, Serialize};

    use crate::dht::{
        now,
        rpc::{Blake3Immutable, Id, Kind, Inline},
    };

    #[rpc_requests(message = ApiMessage)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ApiProto {
        /// NodesSeen can only be called for node ids, but we don't bother
        /// to provide AddrInfo here since the routing table does not store it.
        #[rpc(tx = NoSender)]
        #[wrap(NodesSeen)]
        NodesSeen { ids: Vec<EndpointId> },
        /// NodesDead can only be called for node ids, but we don't bother
        /// to provide AddrInfo here since the routing table does not store it.
        #[rpc(tx = NoSender)]
        #[wrap(NodesDead)]
        NodesDead { ids: Vec<EndpointId> },
        #[rpc(tx = oneshot::Sender<Vec<EndpointId>>)]
        #[wrap(Lookup)]
        Lookup {
            initial: Option<Vec<EndpointId>>,
            id: Id,
        },
        #[rpc(tx = mpsc::Sender<EndpointId>)]
        #[wrap(NetworkPut)]
        NetworkPut { id: Id, value: Inline },
        #[rpc(tx = mpsc::Sender<(EndpointId, Inline)>)]
        #[wrap(NetworkGet)]
        NetworkGet {
            id: Id,
            kind: Kind,
            seed: Option<NonZeroU64>,
            n: Option<NonZeroU64>,
        },
        /// Get the routing table for testing
        #[rpc(tx = oneshot::Sender<Vec<Vec<EndpointId>>>)]
        #[wrap(GetRoutingTable)]
        GetRoutingTable,
        /// Get storage stats for testing
        #[rpc(tx = oneshot::Sender<BTreeMap<Id, BTreeMap<Kind, usize>>>)]
        #[wrap(GetStorageStats)]
        GetStorageStats,
        /// Perform a self lookup
        #[rpc(tx = oneshot::Sender<()>)]
        #[wrap(SelfLookup)]
        SelfLookup,
        /// Perform a random lookup
        #[rpc(tx = oneshot::Sender<()>)]
        #[wrap(RandomLookup)]
        RandomLookup,
        /// Perform a candidate lookup
        #[rpc(tx = oneshot::Sender<()>)]
        #[wrap(CandidateLookup)]
        CandidateLookup,
    }

    #[derive(Debug, Clone)]
    pub struct ApiClient(pub(crate) Arc<irpc::Client<ApiProto>>);

    impl ApiClient {
        /// notify the node that we have just seen these nodes.
        ///
        /// The impl should add these nodes to the routing table.
        pub async fn nodes_seen(&self, ids: &[EndpointId]) -> irpc::Result<()> {
            self.0.notify(NodesSeen { ids: ids.to_vec() }).await
        }

        /// notify the node that we have tried to contact these nodes and have not gotten a response.
        ///
        /// The impl can either clean these nodes from its routing table immediately or after a repeat offense.
        pub async fn nodes_dead(&self, ids: &[EndpointId]) -> irpc::Result<()> {
            self.0.notify(NodesDead { ids: ids.to_vec() }).await
        }

        pub async fn get_storage_stats(&self) -> irpc::Result<BTreeMap<Id, BTreeMap<Kind, usize>>> {
            self.0.rpc(GetStorageStats).await
        }

        pub async fn get_routing_table(&self) -> irpc::Result<Vec<Vec<EndpointId>>> {
            self.0.rpc(GetRoutingTable).await
        }

        pub async fn lookup(
            &self,
            id: Id,
            initial: Option<Vec<EndpointId>>,
        ) -> irpc::Result<Vec<EndpointId>> {
            self.0.rpc(Lookup { id, initial }).await
        }

        pub async fn get_immutable(&self, hash: blake3::Hash) -> irpc::Result<Option<Vec<u8>>> {
            let id = Id::from(*hash.as_bytes());
            let mut rx = self
                .0
                .server_streaming(
                    NetworkGet {
                        id,
                        kind: Kind::Blake3Immutable,
                        seed: None,
                        n: Some(NonZeroU64::new(1).unwrap()),
                    },
                    32,
                )
                .await?;
            loop {
                match rx.recv().await {
                    Ok(Some((_, value))) => {
                        let Inline::Blake3Immutable(Blake3Immutable { data, .. }) = value else {
                            continue; // Skip non-Blake3Immutable values
                        };
                        if blake3::hash(&data) == hash {
                            return Ok(Some(data));
                        } else {
                            continue; // Hash mismatch, skip this value
                        }
                    }
                    Ok(None) => {
                        break Ok(None);
                    }
                    Err(e) => {
                        break Err(e.into());
                    }
                }
            }
        }

        pub async fn put_immutable(
            &self,
            value: &[u8],
        ) -> irpc::Result<(blake3::Hash, Vec<EndpointId>)> {
            let hash = blake3::hash(value);
            let id = Id::from(*hash.as_bytes());
            let mut rx = self
                .0
                .server_streaming(
                    NetworkPut {
                        id,
                        value: Inline::Blake3Immutable(Blake3Immutable {
                            timestamp: now(),
                            data: value.to_vec(),
                        }),
                    },
                    32,
                )
                .await?;
            let mut res = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(Some(id)) => res.push(id),
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
            Ok((hash, res))
        }

        /// Announce that this node provides the blob with the given blake3 hash.
        pub async fn announce_provider(
            &self,
            hash: blake3::Hash,
            endpoint_id: EndpointId,
        ) -> irpc::Result<Vec<EndpointId>> {
            let id = Id::from(*hash.as_bytes());
            let mut id_bytes = [0u8; 32];
            id_bytes.copy_from_slice(endpoint_id.as_bytes());
            let mut rx = self
                .0
                .server_streaming(
                    NetworkPut {
                        id,
                        value: Inline::Blake3Provider(super::rpc::Blake3Provider {
                            timestamp: now(),
                            endpoint_id: id_bytes,
                        }),
                    },
                    32,
                )
                .await?;
            let mut stored_on = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(Some(id)) => stored_on.push(id),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            Ok(stored_on)
        }

        /// Find nodes that provide the blob with the given blake3 hash.
        pub async fn find_providers(
            &self,
            hash: blake3::Hash,
        ) -> irpc::Result<Vec<EndpointId>> {
            let id = Id::from(*hash.as_bytes());
            let mut rx = self
                .0
                .server_streaming(
                    NetworkGet {
                        id,
                        kind: Kind::Blake3Provider,
                        seed: None,
                        n: None,
                    },
                    32,
                )
                .await?;
            let mut providers = Vec::new();
            loop {
                match rx.recv().await {
                    Ok(Some((_from, value))) => {
                        if let Inline::Blake3Provider(super::rpc::Blake3Provider { endpoint_id, .. }) = value {
                            if let Ok(key) = iroh_base::PublicKey::from_bytes(&endpoint_id) {
                                providers.push(EndpointId::from(key));
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            Ok(providers)
        }

        pub async fn self_lookup(&self) {
            self.0.rpc(SelfLookup).await.ok();
        }

        pub async fn random_lookup(&self) {
            self.0.rpc(RandomLookup).await.ok();
        }

        pub async fn candidate_lookup(&self) {
            self.0.rpc(CandidateLookup).await.ok();
        }

        pub fn downgrade(&self) -> WeakApiClient {
            WeakApiClient(Arc::downgrade(&self.0))
        }
    }

    #[derive(Debug, Clone)]
    pub struct WeakApiClient(pub(crate) Weak<irpc::Client<ApiProto>>);

    impl WeakApiClient {
        pub fn upgrade(&self) -> irpc::Result<ApiClient> {
            self.0
                .upgrade()
                .map(ApiClient)
                .ok_or(irpc::Error::Send {
                    source: irpc::channel::SendError::ReceiverClosed { meta: Default::default() },
                    meta: Default::default(),
                })
        }

        pub async fn nodes_dead(&self, ids: &[EndpointId]) -> irpc::Result<()> {
            self.upgrade()?.nodes_dead(ids).await
        }

        pub async fn nodes_seen(&self, ids: &[EndpointId]) -> irpc::Result<()> {
            self.upgrade()?.nodes_seen(ids).await
        }

        pub(crate) async fn self_lookup_periodic(self, interval: Duration) {
            loop {
                tokio::time::sleep(interval).await;
                let Ok(api) = self.upgrade() else {
                    return;
                };
                api.self_lookup().await;
            }
        }

        pub(crate) async fn random_lookup_periodic(self, interval: Duration) {
            loop {
                tokio::time::sleep(interval).await;
                let Ok(api) = self.upgrade() else {
                    return;
                };
                api.random_lookup().await;
            }
        }

        pub(crate) async fn candidate_lookup_periodic(self, interval: Duration) {
            loop {
                tokio::time::sleep(interval).await;
                let Ok(api) = self.upgrade() else {
                    return;
                };
                api.candidate_lookup().await;
            }
        }
    }
}
pub use api::ApiClient;
use tracing::{error, info, warn};

mod routing {
    use std::{
        fmt,
        ops::{Index, IndexMut},
    };

    use arrayvec::ArrayVec;
    use iroh::EndpointId;

    use super::rpc::Id;

    pub const K: usize = 20; // Bucket size
    pub const ALPHA: usize = 3; // Concurrency parameter
    pub const BUCKET_COUNT: usize = 256; // For 256-bit keys

    /// Calculate XOR distance between two 32-byte values
    fn xor(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = a[i] ^ b[i];
        }
        result
    }

    /// Count leading zero bits in a 32-byte array
    fn leading_zeros(data: &[u8; 32]) -> usize {
        for (byte_idx, &byte) in data.iter().enumerate() {
            if byte != 0 {
                return byte_idx * 8 + byte.leading_zeros() as usize;
            }
        }
        256 // All zeros
    }

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Distance([u8; 32]);

    impl Distance {
        pub fn between(a: &[u8; 32], b: &[u8; 32]) -> Self {
            Self(xor(a, b))
        }

        /// This is the inverse of between.
        ///
        /// Distance::between(&x, &y).to_node(&y) == x
        pub fn inverse(&self, b: &[u8; 32]) -> [u8; 32] {
            xor(&self.0, b)
        }

        pub const MAX: Self = Self([u8::MAX; 32]);
    }

    impl fmt::Debug for Distance {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Distance({self})")
        }
    }

    impl fmt::Display for Distance {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", hex::encode(self.0))
        }
    }

    #[derive(Debug, Clone, Default)]
    pub struct KBucket {
        nodes: ArrayVec<EndpointId, K>,
    }

    impl KBucket {
        const EMPTY: &'static Self = &Self {
            nodes: ArrayVec::new_const(),
        };

        fn new() -> Self {
            Self {
                nodes: ArrayVec::new(),
            }
        }

        pub fn add_node(&mut self, node: EndpointId) -> bool {
            // Check if node already exists and update it
            for existing in &mut self.nodes {
                if existing == &node {
                    return true; // Updated existing node
                }
            }

            // Add new node if space available
            if self.nodes.len() < K {
                self.nodes.push(node);
                return true;
            }

            false // Bucket full
        }

        fn remove_node(&mut self, id: &EndpointId) {
            self.nodes.retain(|n| n != id);
        }

        pub fn nodes(&self) -> &[EndpointId] {
            &self.nodes
        }
    }

    #[derive(Debug)]
    pub struct RoutingTable {
        pub buckets: Buckets,
        pub local_id: EndpointId,
    }

    #[derive(Debug, Clone, Default)]
    pub struct Buckets(Vec<KBucket>);

    impl Buckets {
        pub fn iter(&self) -> std::slice::Iter<'_, KBucket> {
            self.0.iter()
        }
    }

    impl Index<usize> for Buckets {
        type Output = KBucket;
        fn index(&self, index: usize) -> &Self::Output {
            if index >= BUCKET_COUNT {
                panic!("Bucket index out of range: {index} >= {}", self.0.len());
            }
            if index >= self.0.len() {
                return &KBucket::EMPTY;
            }
            &self.0[index]
        }
    }

    impl IndexMut<usize> for Buckets {
        fn index_mut(&mut self, index: usize) -> &mut Self::Output {
            if index >= BUCKET_COUNT {
                panic!("Bucket index out of range: {index} >= {}", self.0.len());
            }
            if index >= self.0.len() {
                self.0.resize(index + 1, KBucket::new());
            }
            &mut self.0[index]
        }
    }

    impl RoutingTable {
        pub fn new(local_id: EndpointId, buckets: Option<Buckets>) -> Self {
            let buckets = buckets
                .map(|mut buckets| {
                    for bucket in buckets.0.iter_mut() {
                        bucket.nodes.retain(|n| n != &local_id);
                    }
                    buckets
                })
                .unwrap_or_default();
            Self { buckets, local_id }
        }

        /// Get the bucket index for a given target ID.
        ///
        /// Contrary to the normal definition, we have bucket 0 be the furthest
        /// xor distance, and bucket 255 be the closest. This means that buckets
        /// are filled from the start of the array.
        ///
        /// For randomly chosen node ids, it is astronomically unlikely that
        /// high buckets are filled at all, even for giant DHTs.
        ///
        /// Returns None if the target is the same as the local ID.
        fn bucket_index(&self, target: &[u8; 32]) -> Option<usize> {
            let distance = xor(self.local_id.as_bytes(), target);
            let zeros = leading_zeros(&distance);
            if zeros >= BUCKET_COUNT {
                None
            } else {
                Some(zeros)
            }
        }

        pub(crate) fn contains(&self, id: &EndpointId) -> bool {
            let Some(bucket_idx) = self.bucket_index(id.as_bytes()) else {
                return false;
            };
            self.buckets[bucket_idx]
                .nodes()
                .iter()
                .any(|node| node == id)
        }

        pub fn add_node(&mut self, node: EndpointId) -> bool {
            let Some(bucket_idx) = self.bucket_index(node.as_bytes()) else {
                return false;
            };
            self.buckets[bucket_idx].add_node(node)
        }

        pub(crate) fn remove_node(&mut self, id: &EndpointId) {
            let Some(bucket_idx) = self.bucket_index(id.as_bytes()) else {
                return;
            };
            self.buckets[bucket_idx].remove_node(id);
        }

        pub fn nodes(&self) -> impl Iterator<Item = &EndpointId> {
            self.buckets.iter().flat_map(|bucket| bucket.nodes())
        }

        pub fn find_closest_nodes(&self, target: &Id, k: usize) -> Vec<EndpointId> {
            // this does a brute force scan, but even so it should be very fast.
            // xor is basically free, and comparing distances as well.
            // so the most expensive thing is probably the memory allocation.
            //
            // for a full routing table, this would be 256*20*32 = 163840 bytes.
            let mut candidates = Vec::with_capacity(self.nodes().count());
            candidates.extend(
                self.nodes()
                    .map(|node| Distance::between(target, node.as_bytes())),
            );
            if k < candidates.len() {
                candidates.select_nth_unstable(k - 1);
                candidates.truncate(k);
            }
            candidates.sort_unstable();

            candidates
                .into_iter()
                .map(|dist| {
                    EndpointId::from_bytes(&dist.inverse(target))
                        .expect("inverse called with different target than between")
                })
                .collect()
        }
    }
}

#[doc(hidden)]
pub mod bench_exports {
    pub use crate::dht::{
        routing::{Buckets, KBucket, RoutingTable},
        rpc::Id,
    };
}

use crate::dht::{
    api::{ApiMessage, Lookup, NetworkGet, NetworkPut, WeakApiClient},
    pool::ClientPool,
    routing::{ALPHA, BUCKET_COUNT, Buckets, Distance, K, RoutingTable},
    rpc::{Id, Kind, RpcClient, RpcMessage, SetResponse, Inline},
    u256::U256,
};

struct Node {
    routing_table: RoutingTable,
    storage: MemStorage,
}

impl Node {
    fn id(&self) -> &EndpointId {
        &self.routing_table.local_id
    }
}

struct MemStorage {
    /// The DHT data storage, mapping keys to values.
    /// Separated by kind to allow for efficient retrieval.
    data: BTreeMap<Id, BTreeMap<Kind, IndexSet<Inline>>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// Set a value for a key.
    fn set(&mut self, key: Id, value: Inline) {
        let kind = value.kind();
        self.data
            .entry(key)
            .or_default()
            .entry(kind)
            .or_default()
            .insert(value);
    }

    /// Get all values of a certain kind for a key.
    fn get_all(&self, key: &Id, kind: &Kind) -> Option<&IndexSet<Inline>> {
        self.data.get(key).and_then(|kinds| kinds.get(kind))
    }
}

mod u256 {
    #![allow(clippy::needless_range_loop)]
    use std::ops::{BitAnd, BitOr, BitXor, Deref, Not, Shl, Shr};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct U256([u8; 32]);

    impl Deref for U256 {
        type Target = [u8; 32];

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl U256 {
        /// Minimum value (all zeros)
        pub const MIN: U256 = U256([0u8; 32]);

        /// Maximum value (all ones)
        pub const MAX: U256 = U256([0xffu8; 32]);

        /// Create a new U256 from a byte array (little-endian)
        pub fn from_le_bytes(bytes: [u8; 32]) -> Self {
            U256(bytes)
        }

        /// Get the underlying byte array (little-endian)
        #[allow(clippy::wrong_self_convention)]
        pub fn to_le_bytes(&self) -> [u8; 32] {
            self.0
        }

        /// Get the number of leading zeros
        #[allow(dead_code)]
        pub fn leading_zeros(&self) -> u32 {
            let mut count = 0;
            for &byte in self.0.iter().rev() {
                if byte == 0 {
                    count += 8;
                } else {
                    count += byte.leading_zeros();
                    break;
                }
            }
            count
        }
    }

    // Bitwise XOR
    impl BitXor for U256 {
        type Output = Self;

        fn bitxor(self, rhs: Self) -> Self::Output {
            let mut result = [0u8; 32];
            for i in 0..32 {
                result[i] = self.0[i] ^ rhs.0[i];
            }
            U256(result)
        }
    }

    // Bitwise AND
    impl BitAnd for U256 {
        type Output = Self;

        fn bitand(self, rhs: Self) -> Self::Output {
            let mut result = [0u8; 32];
            for i in 0..32 {
                result[i] = self.0[i] & rhs.0[i];
            }
            U256(result)
        }
    }

    // Bitwise OR
    impl BitOr for U256 {
        type Output = Self;

        fn bitor(self, rhs: Self) -> Self::Output {
            let mut result = [0u8; 32];
            for i in 0..32 {
                result[i] = self.0[i] | rhs.0[i];
            }
            U256(result)
        }
    }

    // Bitwise NOT
    impl Not for U256 {
        type Output = Self;

        fn not(self) -> Self::Output {
            let mut result = [0u8; 32];
            for i in 0..32 {
                result[i] = !self.0[i];
            }
            U256(result)
        }
    }

    // Left shift without wraparound
    impl Shl<u32> for U256 {
        type Output = Self;

        fn shl(self, rhs: u32) -> Self::Output {
            if rhs >= 256 {
                return U256::MIN;
            }

            // Split into two u128 values (little-endian)
            let low = u128::from_le_bytes(self.0[0..16].try_into().unwrap());
            let high = u128::from_le_bytes(self.0[16..32].try_into().unwrap());

            let (new_low, new_high) = if rhs >= 128 {
                // Shift more than 128 bits: low becomes 0, high gets low shifted
                let shift_amount = rhs - 128;
                (0, low << shift_amount)
            } else {
                // Normal shift: both parts get shifted, with overflow from low to high
                let overflow_bits = 128 - rhs;
                let new_low = low << rhs;
                let new_high = (high << rhs) | (low >> overflow_bits);
                (new_low, new_high)
            };

            let mut result = [0u8; 32];
            result[0..16].copy_from_slice(&new_low.to_le_bytes());
            result[16..32].copy_from_slice(&new_high.to_le_bytes());

            U256(result)
        }
    }

    // Right shift without wraparound
    impl Shr<u32> for U256 {
        type Output = Self;

        fn shr(self, rhs: u32) -> Self::Output {
            if rhs >= 256 {
                return U256::MIN;
            }

            // Split into two u128 values (little-endian)
            let low = u128::from_le_bytes(self.0[0..16].try_into().unwrap());
            let high = u128::from_le_bytes(self.0[16..32].try_into().unwrap());

            let (new_low, new_high) = if rhs >= 128 {
                // Shift more than 128 bits: high becomes 0, low gets high shifted
                let shift_amount = rhs - 128;
                (high >> shift_amount, 0)
            } else {
                // Normal shift: both parts get shifted, with overflow from high to low
                let overflow_bits = 128 - rhs;
                let new_low = (low >> rhs) | (high << overflow_bits);
                let new_high = high >> rhs;
                (new_low, new_high)
            };

            let mut result = [0u8; 32];
            result[0..16].copy_from_slice(&new_low.to_le_bytes());
            result[16..32].copy_from_slice(&new_high.to_le_bytes());

            U256(result)
        }
    }
}

pub mod pool {
    //! An abstract pool of [`RpcClient`]s.
    //!
    //! This can be implemented with an in-memory implementation for tests,
    //! and with a proper iroh connection pool for real use.

    use std::sync::{Arc, RwLock};

    use iroh::{
        Endpoint, EndpointAddr, EndpointId,
        endpoint::{RecvStream, SendStream},
    };
    use iroh_blobs::util::connection_pool::{ConnectionPool, ConnectionRef};
    use snafu::Snafu;
    use tracing::error;

    use crate::dht::rpc::{RpcClient, WeakRpcClient};

    /// A pool that can efficiently provide clients given a node id, and knows its
    /// own identity.
    ///
    /// For tests, this is just a map from id to client. For production, this will
    /// wrap an iroh Endpoint and have some sort of connection cache.
    pub trait ClientPool: Send + Sync + Clone + Sized + 'static {
        /// Our own node id
        fn id(&self) -> EndpointId;

        /// Adds dialing info to a node id.
        ///
        /// The default impl doesn't add anything.
        fn node_addr(&self, endpoint_id: EndpointId) -> EndpointAddr {
            endpoint_id.into()
        }

        /// Adds dialing info for a node id.
        ///
        /// The default impl doesn't add anything.
        fn add_node_addr(&self, _addr: EndpointAddr) {}

        /// Use the client to perform an operation.
        ///
        /// You must not clone the client out of the closure. If you do, this client
        /// can become unusable at any time!
        fn client(&self, id: EndpointId) -> impl Future<Output = Result<RpcClient, String>> + Send;
    }

    /// Error when a pool can not obtain a client.
    #[derive(Debug, Snafu)]
    pub struct PoolError {
        pub message: String,
    }

    /// A client pool backed by real iroh connections.
    #[derive(Debug, Clone)]
    pub struct IrohPool {
        endpoint: Endpoint,
        inner: ConnectionPool,
        self_client: Arc<RwLock<Option<WeakRpcClient>>>,
    }

    impl IrohPool {
        pub fn new(endpoint: Endpoint, inner: ConnectionPool) -> Self {
            Self {
                endpoint,
                inner,
                self_client: Arc::new(RwLock::new(None)),
            }
        }

        /// Iroh connections to self are not allowed. But when storing or getting values,
        /// it is perfectly reasonable to have the own id as the best location.
        ///
        /// To support this seamlessly, this allows creating a client to self.
        ///
        /// This has to be a weak client since we don't want the pool, which is owned by the
        /// node actor, to keep the node actor alive.
        pub fn set_self_client(&self, client: Option<WeakRpcClient>) {
            let mut self_client = self.self_client.write().unwrap();
            *self_client = client;
        }
    }

    #[derive(Debug, Clone)]
    struct IrohConnection(Arc<ConnectionRef>);

    impl irpc::rpc::RemoteConnection for IrohConnection {
        fn clone_boxed(&self) -> Box<dyn irpc::rpc::RemoteConnection> {
            Box::new(self.clone())
        }

        fn open_bi(
            &self,
        ) -> n0_future::future::Boxed<
            std::result::Result<(SendStream, RecvStream), irpc::RequestError>,
        > {
            let conn = self.0.clone();
            Box::pin(async move {
                let (send, recv) = conn.open_bi().await?;
                Ok((send, recv))
            })
        }

        fn zero_rtt_accepted(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'static>> {
            Box::pin(std::future::ready(false))
        }
    }

    impl ClientPool for IrohPool {
        fn id(&self) -> EndpointId {
            self.endpoint.id()
        }

        fn node_addr(&self, endpoint_id: EndpointId) -> EndpointAddr {
            // TODO: we need to get the info from the endpoint somehow, but as
            // 0.93.0 it is no longer possible.
            //
            // See https://github.com/n0-computer/iroh/issues/3521
            endpoint_id.into()
        }

        fn add_node_addr(&self, addr: EndpointAddr) {
            // don't add self info.
            // this should not happen, but just in case
            if addr.id == self.id() {
                return;
            }
            // don't add useless info.
            if addr.addrs.is_empty() {
                return;
            }
            // Add the address info via the address lookup provider.
            if let Ok(lookup) = self.endpoint.address_lookup() {
                let mem = iroh::address_lookup::MemoryLookup::new();
                mem.add_endpoint_info(addr);
                lookup.add(mem);
            }
        }

        async fn client(&self, endpoint_id: EndpointId) -> Result<RpcClient, String> {
            if endpoint_id == self.id() {
                // If we are trying to connect to ourselves, return the self client if available.
                if let Some(client) = self.self_client.read().unwrap().clone() {
                    return client
                        .upgrade()
                        .ok_or_else(|| "Self client is no longer available".to_string());
                } else {
                    error!("Self client not set");
                    return Err("Self client not set".to_string());
                }
            }
            let connection = self
                .inner
                .get_or_connect(endpoint_id)
                .await
                .map_err(|e| format!("Failed to connect: {e}"));
            let connection = connection?;
            let client = RpcClient::new(irpc::Client::boxed(IrohConnection(Arc::new(connection))));
            Ok(client)
        }
    }
}

/// State of the actor that is required in the async handlers
#[derive(Debug, Clone)]
struct State<P> {
    /// ability to send messages to ourselves, e.g. to update the routing table
    api: WeakApiClient,
    /// client pool
    pool: P,
    /// configuration
    config: Config,
}

struct Candidates {
    ids: VecDeque<EndpointId>,
    max_size: usize,
}

impl Candidates {
    fn new(max_size: usize) -> Self {
        Self {
            ids: VecDeque::new(),
            max_size,
        }
    }

    /// Adds a candidate, dedups, and maintains the max size.
    fn add(&mut self, id: EndpointId) {
        self.ids.retain(|x| x != &id);
        self.ids.push_front(id);
        while self.ids.len() > self.max_size {
            self.ids.pop_back();
        }
    }

    /// Returns the candidates, most recent first, and clears the set
    fn clear_and_take(&mut self) -> Vec<EndpointId> {
        let res = self.ids.iter().cloned().collect();
        self.ids.clear();
        res
    }
}

struct Actor<P> {
    node: Node,
    /// receiver for rpc messages from the network
    rpc_rx: mpsc::Receiver<RpcMessage>,
    /// receiver for api messages from in process or local network
    api_rx: mpsc::Receiver<ApiMessage>,
    /// ongoing tasks
    tasks: JoinSet<()>,
    /// state
    state: State<P>,
    /// candidates for inclusion in the routing table
    candidates: Option<Candidates>,
    /// Rng for random lookups
    rng: rand::rngs::StdRng,
}

/// Dht lookup config
///
/// Note that while transient, parallelism and alpha are parameters that you can
/// modify to suit your needs, you very rarely want to modify the k parameter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// DHT parameter K
    k: usize,
    /// DHT parameter ALPHA
    alpha: usize,
    /// Parallelism for the set or getall requests once we have found the k
    /// closest nodes.
    parallelism: usize,
    /// Whether the requester is a transient node.
    transient: bool,
    /// Random number generator seed.
    rng_seed: Option<[u8; 32]>,
    /// Lookup strategies.
    lookup_strategies: LookupStrategies,
}

pub mod config {
    //! Detailed configuration for the DHT.
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct LookupStrategies {
        /// Random lookup strategy.
        pub random: Option<RandomLookupStrategy>,
        /// Self lookup strategy.
        pub self_id: Option<SelfLookupStrategy>,
        /// Candidate lookup strategy.
        pub candidate: Option<CandidateLookupStrategy>,
    }

    impl LookupStrategies {
        /// No lookup strategies.
        ///
        /// This is good for testing, since it allows you to trigger lookups
        /// manually. But don't use this in prod!
        pub fn none() -> Self {
            Self {
                random: None,
                self_id: None,
                candidate: None,
            }
        }
    }

    /// This is the mechanism for adding new nodes to a DHT.
    ///
    /// When we find a node that is not in our routing table, we don't immediately
    /// update our routing table. It could be a node that claims to be permanent
    /// but is transient. Instead, we add it to a list of candidates for inclusion
    /// that gets used periodically.
    ///
    /// We perform random lookups with the candidate nodes as initial peers. This
    /// will cause them to be validated by sending them a FindNode query, and as
    /// a side effect of validating we update our routing table.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    pub struct CandidateLookupStrategy {
        pub max_lookups: usize,
        pub interval: Duration,
    }

    /// Perform a periodic self-lookup.
    ///
    /// This is useful to update the buckets close to self. Random lookups will
    /// rarely hit these.

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    pub struct SelfLookupStrategy {
        pub interval: Duration,
    }

    /// Perform a periodic random lookup.
    ///
    /// This is useful to update the buckets that are far away from self.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    pub struct RandomLookupStrategy {
        pub interval: Duration,
        pub blended: bool,
    }
}
use config::*;

impl Config {
    /// Configuration for a transient node that does not want to be included
    /// in the routing tables of its peers.
    pub fn transient() -> Self {
        Self {
            transient: true,
            ..Default::default()
        }
    }

    /// Configuration for a persistent node that wants to be included
    /// in the routing tables of its peers.
    pub fn persistent() -> Self {
        Self {
            transient: false,
            ..Default::default()
        }
    }

    pub fn candidate_lookup_strategy(mut self, value: CandidateLookupStrategy) -> Self {
        self.lookup_strategies.candidate = Some(value);
        self
    }

    pub fn random_lookup_strategy(mut self, value: RandomLookupStrategy) -> Self {
        self.lookup_strategies.random = Some(value);
        self
    }

    pub fn self_lookup_strategy(mut self, value: SelfLookupStrategy) -> Self {
        self.lookup_strategies.self_id = Some(value);
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            k: K,
            alpha: ALPHA,
            parallelism: 4,
            transient: true,
            lookup_strategies: LookupStrategies {
                random: None,
                self_id: None,
                candidate: None,
            },
            rng_seed: None,
        }
    }
}

impl<P> Actor<P>
where
    P: ClientPool,
{
    fn new(
        node: Node,
        rx: mpsc::Receiver<RpcMessage>,
        pool: P,
        config: Config,
    ) -> (Self, ApiClient) {
        let (api_tx, internal_rx) = mpsc::channel(32);
        let api = ApiClient(Arc::new(LocalSender::from(api_tx).into()));
        let mut tasks = JoinSet::new();
        let state = State {
            api: api.downgrade(),
            pool,
            config: config.clone(),
        };
        tasks.spawn(state.clone().notify_self());
        (
            Self {
                node,
                rpc_rx: rx,
                api_rx: internal_rx,
                tasks,
                state,
                candidates: config
                    .lookup_strategies
                    .candidate
                    .map(|s| Candidates::new(s.max_lookups * config.k)),
                rng: config
                    .rng_seed
                    .map(StdRng::from_seed)
                    .unwrap_or(StdRng::from_entropy()),
            },
            api,
        )
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                msg = self.rpc_rx.recv() => {
                    if let Ok(Some(msg)) = msg {
                        self.handle_rpc(msg).await;
                    } else {
                        break;
                    }
                }
                msg = self.api_rx.recv() => {
                    if let Ok(Some(msg)) = msg {
                        self.handle_api(msg).await;
                    } else {
                        break;
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Err(e) = res {
                        error!("Task failed: {:?}", e);
                    }
                }
            }
        }
    }

    /// Handle a single API message
    async fn handle_api(&mut self, message: ApiMessage) {
        match message {
            ApiMessage::NodesSeen(msg) => {
                for id in msg.ids.iter().copied() {
                    self.node.routing_table.add_node(id);
                }
            }
            ApiMessage::NodesDead(msg) => {
                for id in msg.ids.iter() {
                    self.node.routing_table.remove_node(id);
                }
            }
            ApiMessage::Lookup(msg) => {
                let initial = msg
                    .initial
                    .clone()
                    .unwrap_or_else(|| self.node.routing_table.find_closest_nodes(&msg.id, K));
                self.tasks
                    .spawn(self.state.clone().lookup(initial, msg.inner, msg.tx));
            }
            ApiMessage::NetworkGet(msg) => {
                // perform a network get by calling the iterative search using the closest
                // nodes from the local routing table, then performing individual requests
                // for the resulting k closest live nodes.
                let initial = self.node.routing_table.find_closest_nodes(&msg.id, K);
                self.tasks
                    .spawn(self.state.clone().network_get(initial, msg.inner, msg.tx));
            }
            ApiMessage::NetworkPut(msg) => {
                // perform a network put by calling the iterative search using the closest
                // nodes from the local routing table, then performing individual requests
                // for the resulting k closest live nodes.
                let initial = self.node.routing_table.find_closest_nodes(&msg.id, K);
                self.tasks
                    .spawn(self.state.clone().network_put(initial, msg.inner, msg.tx));
            }
            ApiMessage::GetRoutingTable(msg) => {
                let table = self
                    .node
                    .routing_table
                    .buckets
                    .iter()
                    .map(|bucket| bucket.nodes().to_vec())
                    .collect();
                msg.tx.send(table).await.ok();
            }
            ApiMessage::GetStorageStats(msg) => {
                // Collect storage stats, mapping Id to Kind to count of values
                let mut stats = BTreeMap::new();
                for (key, kinds) in &self.node.storage.data {
                    let kind_stats = kinds
                        .iter()
                        .map(|(kind, values)| (*kind, values.len()))
                        .collect();
                    stats.insert(*key, kind_stats);
                }
                msg.tx.send(stats).await.ok();
            }
            ApiMessage::SelfLookup(msg) => {
                let id = self.state.pool.id().into();
                // todo: choose initial to be farthest away from self, otherwise
                // the self lookup won't be very useful.
                let api = self.state.api.clone();
                self.tasks.spawn(async move {
                    let Ok(api) = api.upgrade() else {
                        return;
                    };
                    api.lookup(id, None).await.ok();
                    msg.tx.send(()).await.ok();
                });
            }
            ApiMessage::RandomLookup(msg) => {
                // bucket up to which the node should overlap with self.
                // 0 is fully random, 256 is just self.
                let blended = false;
                let id = if blended {
                    let bucket = self.rng.gen_range::<u32, _>(0..BUCKET_COUNT as u32 + 2);
                    let this = U256::from_le_bytes(*self.node.id().as_bytes());
                    let random = U256::from_le_bytes(self.rng.r#gen());
                    let res = blend(this, random, bucket);
                    Id::from(res.to_le_bytes())
                } else {
                    Id::from(self.rng.r#gen::<[u8; 32]>())
                };
                let api = self.state.api.clone();
                self.tasks.spawn(async move {
                    let Ok(api) = api.upgrade() else {
                        return;
                    };
                    api.lookup(id, None).await.ok();
                    msg.tx.send(()).await.ok();
                });
            }
            ApiMessage::CandidateLookup(msg) => {
                if self.state.config.lookup_strategies.candidate.is_none() {
                    warn!(
                        "Received CandidateLookup request, but no candidate lookup strategy is configured"
                    );
                    return;
                };
                let Some(candidates) = self.candidates.as_mut() else {
                    warn!("Received CandidateLookup request, but no candidates are being tracked");
                    return;
                };
                // use the most recent `max_lookups * k` candidates
                let chosen = candidates.clear_and_take();
                // perform a random lookup using the candidates as initial.
                //
                let api = self.state.api.clone();
                let groups = chosen
                    .chunks(self.state.config.k)
                    .map(|chunk| {
                        let id = Id::from(self.rng.r#gen::<[u8; 32]>());
                        (id, chunk.to_vec())
                    })
                    .collect::<Vec<_>>();
                self.tasks.spawn(async move {
                    let Ok(api) = api.upgrade() else {
                        return;
                    };
                    // this will check if they exist. If yes, they will be added to the routing table.
                    for (id, ids) in groups {
                        api.lookup(id, Some(ids)).await.ok();
                    }
                    msg.tx.send(()).await.ok();
                });
            }
        }
    }

    async fn handle_rpc(&mut self, message: RpcMessage) {
        match message {
            RpcMessage::Set(msg) => {
                // just set the value in the local storage
                //
                // TODO: check if the data is expired or invalid and return the
                // appropriate error response.
                //
                // Sanity check that this node is a good node to store
                // the data at, using the local routing table, and if not return
                // a SetResponse::ErrDistance.
                let ids = self
                    .node
                    .routing_table
                    .find_closest_nodes(&msg.key, self.state.config.k);
                let self_dist = Distance::between(self.node.id().as_bytes(), &msg.key);
                // if we know k nodes that are closer to the key than we are, we don't want to store
                // the data!
                if ids.len() >= self.state.config.k
                    && ids.iter().all(|id| {
                        Distance::between(self.node.id().as_bytes(), id.as_bytes()) < self_dist
                    })
                {
                    msg.tx.send(SetResponse::ErrDistance).await.ok();
                    return;
                }
                self.node.storage.set(msg.key, msg.value.clone());
                msg.tx.send(SetResponse::Ok).await.ok();
            }
            RpcMessage::GetAll(msg) => {
                // Get all values, applying the provided filters and limits.
                let Some(values) = self.node.storage.get_all(&msg.key, &msg.kind) else {
                    return;
                };
                // Randomize the order of the results given the provided seed
                if let Some(seed) = msg.seed {
                    let mut rng = rand::rngs::StdRng::seed_from_u64(seed.get());
                    let n = msg.n.map(|x| x.get()).unwrap_or(values.len() as u64) as usize;
                    let indices = sample(&mut rng, values.len(), n);
                    for i in indices {
                        if let Some(value) = values.get_index(i)
                            && msg.tx.send(value.clone()).await.is_err()
                        {
                            break;
                        }
                    }
                } else {
                    // just send them in whatever order they return from the store.
                    for value in values {
                        if msg.tx.send(value.clone()).await.is_err() {
                            break;
                        }
                    }
                }
            }
            RpcMessage::FindNode(msg) => {
                // call local find_node and just return the results
                let ids = self
                    .node
                    .routing_table
                    .find_closest_nodes(&msg.id, self.state.config.k)
                    .into_iter()
                    .take(self.state.config.k) // should not be needed, but just in case
                    .map(|id| self.state.pool.node_addr(id))
                    .collect();
                if let Some(requester) = msg.requester {
                    self.add_candidate(requester);
                }
                msg.tx.send(ids).await.ok();
            }
        }
    }

    fn add_candidate(&mut self, id: EndpointId) {
        if self.state.config.transient {
            warn!("Received FindNode request for transient node");
            return;
        }
        if self.node.routing_table.contains(&id) {
            return;
        }
        let Some(candidates) = &mut self.candidates else {
            // candidate tracking is not enabled
            return;
        };
        // add it to the candidates for routing table inclusion
        candidates.add(id);
    }
}

impl<P: ClientPool> State<P> {
    async fn lookup(self, initial: Vec<EndpointId>, msg: Lookup, tx: oneshot::Sender<Vec<EndpointId>>) {
        let ids = self.clone().iterative_find_node(msg.id, initial).await;
        tx.send(ids).await.ok();
    }

    async fn network_put(self, initial: Vec<EndpointId>, msg: NetworkPut, tx: mpsc::Sender<EndpointId>) {
        let ids = self.clone().iterative_find_node(msg.id, initial).await;
        stream::iter(ids)
            .for_each_concurrent(self.config.parallelism, |id| {
                let pool = self.pool.clone();
                let value = msg.value.clone();
                let tx = tx.clone();
                async move {
                    let Ok(client) = pool.client(id).await else {
                        return;
                    };
                    if client.set(msg.id, value).await.is_ok() {
                        tx.send(id).await.ok();
                    }
                    drop(client);
                }
            })
            .await;
    }

    async fn network_get(
        self,
        initial: Vec<EndpointId>,
        msg: NetworkGet,
        tx: mpsc::Sender<(EndpointId, Inline)>,
    ) {
        let ids = self.clone().iterative_find_node(msg.id, initial).await;
        stream::iter(ids)
            .for_each_concurrent(self.config.parallelism, |id| {
                let pool = self.pool.clone();
                let tx = tx.clone();
                let msg = NetworkGet {
                    id: msg.id,
                    kind: msg.kind,
                    seed: msg.seed,
                    n: msg.n,
                };
                async move {
                    let Ok(client) = pool.client(id).await else {
                        return;
                    };
                    // Get all values of the specified kind for the key
                    let Ok(mut rx) = client.get_all(msg.id, msg.kind, msg.seed, msg.n).await else {
                        return;
                    };
                    while let Ok(Some(value)) = rx.recv().await {
                        if tx.send((id, value)).await.is_err() {
                            break;
                        }
                    }
                    drop(client);
                }
            })
            .await;
    }

    async fn query_one(&self, id: EndpointId, target: Id) -> Result<Vec<EndpointId>, &'static str> {
        let requester = if self.config.transient {
            None
        } else {
            Some(self.pool.id())
        };

        let client = self
            .pool
            .client(id)
            .await
            .map_err(|_| "Error getting client")?;
        let infos = client
            .find_node(target, requester)
            .await
            .map_err(|_| "Failed to query node");
        if let Err(e) = &infos {
            info!(%id, "Failed to query node: {e}");
            return Err("Failed to query node");
        }
        let infos = infos?;
        drop(client);
        let ids = infos.iter().map(|info| info.id).collect();
        for info in infos {
            self.pool.add_node_addr(info);
        }
        Ok(ids)
    }

    async fn iterative_find_node(self, target: Id, initial: Vec<EndpointId>) -> Vec<EndpointId> {
        let mut candidates = initial
            .into_iter()
            .filter(|addr| *addr != self.pool.id())
            .map(|id| (Distance::between(&target, id.as_bytes()), id))
            .collect::<BTreeSet<_>>();
        let mut queried = HashSet::new();
        let mut tasks = FuturesUnordered::new();
        let mut result = BTreeSet::new();
        queried.insert(self.pool.id());
        result.insert((
            Distance::between(self.pool.id().as_bytes(), &target),
            self.pool.id(),
        ));

        loop {
            for _ in 0..self.config.alpha {
                let Some(pair @ (_, id)) = candidates.pop_first() else {
                    break;
                };
                queried.insert(id);
                let fut = self.query_one(id, target);
                tasks.push(async move { (pair, fut.await) });
            }

            while let Some((pair @ (_, id), cands)) = tasks.next().await {
                let Ok(cands) = cands else {
                    self.api.nodes_dead(&[id]).await.ok();
                    continue;
                };
                for cand in cands {
                    let dist = Distance::between(&target, cand.as_bytes());
                    if !queried.contains(&cand) {
                        candidates.insert((dist, cand));
                    }
                }
                self.api.nodes_seen(&[id]).await.ok();
                result.insert(pair);
            }

            // truncate the result to k.
            while result.len() > self.config.k {
                result.pop_last();
            }

            // find the k-th best distance
            let kth_best_distance = result
                .iter()
                .nth(self.config.k - 1)
                .map(|(dist, _)| *dist)
                .unwrap_or(Distance::MAX);

            // true if we candidates that are better than distance for result[k-1].
            let has_closer_candidates = candidates
                .first()
                .map(|(dist, _)| *dist < kth_best_distance)
                .unwrap_or_default();

            if !has_closer_candidates {
                break;
            }
        }

        // result already has size <= k
        result.into_iter().map(|(_, id)| id).collect()
    }

    /// Task that sends messages to self in periodic intervals for routing
    /// table maintenance, if configured.
    async fn notify_self(self) {
        let mut self_lookup = MaybeFuture::None;
        let mut random_lookup = MaybeFuture::None;
        let mut candidate_lookup = MaybeFuture::None;
        if let Some(strategy) = &self.config.lookup_strategies.self_id {
            let api = self.api.clone();
            self_lookup = MaybeFuture::Some(api.self_lookup_periodic(strategy.interval));
        }
        if let Some(strategy) = &self.config.lookup_strategies.random {
            let api = self.api.clone();
            random_lookup = MaybeFuture::Some(api.random_lookup_periodic(strategy.interval));
        }
        if let Some(strategy) = &self.config.lookup_strategies.candidate {
            let api = self.api.clone();
            candidate_lookup = MaybeFuture::Some(api.candidate_lookup_periodic(strategy.interval));
        }
        tokio::pin!(self_lookup, random_lookup, candidate_lookup);
        loop {
            tokio::select! {
                _ = &mut self_lookup => {
                }
                _ = &mut random_lookup => {
                }
                _ = &mut candidate_lookup => {
                }
            }
        }
    }
}

/// Blend two values.
///
/// n is the number of bits different from a
/// n=0, just a
/// n=1, smallest bit is different from a
/// n=2, second smallest bit is different from a, smallest bit is from b
/// n=256, highest bit is different from a, all others from b
/// n>256, just b
fn blend(a: U256, b: U256, n: u32) -> U256 {
    if n >= 256 {
        return b;
    }
    let a_mask = U256::MAX << n;
    let b_mask = (!a_mask) >> 1;
    let xor_mask = !(a_mask | b_mask);
    a & a_mask | a ^ xor_mask | b & b_mask
}

fn now() -> u64 {
    UNIX_EPOCH.elapsed().unwrap().as_secs()
}

/// Creates a DHT node
pub fn create_node<P: ClientPool>(
    id: EndpointId,
    pool: P,
    bootstrap: Vec<EndpointId>,
    config: Config,
) -> (RpcClient, ApiClient) {
    create_node_impl(id, pool, bootstrap, None, config)
}

/// Create a node, with the option to set the initial routing table buckets
fn create_node_impl<P: ClientPool>(
    id: EndpointId,
    pool: P,
    bootstrap: Vec<EndpointId>,
    buckets: Option<Buckets>,
    config: Config,
) -> (RpcClient, ApiClient) {
    let mut node = Node {
        routing_table: RoutingTable::new(id, buckets),
        storage: MemStorage::new(),
    };
    for bootstrap_id in bootstrap {
        if bootstrap_id != id {
            node.routing_table.add_node(bootstrap_id);
        }
    }
    let (tx, rx) = mpsc::channel(32);
    let (actor, api) = Actor::<P>::new(node, rx, pool, config);
    tokio::spawn(actor.run());
    (RpcClient::new(irpc::Client::local(tx)), api)
}

// ── ContentDiscovery implementation for iroh-blobs ───────────────────

/// A [`ContentDiscovery`][cd] implementation backed by the iroh DHT.
///
/// Wraps an [`ApiClient`] and uses Blake3Provider records to discover
/// which nodes have a given blob.
///
/// [cd]: iroh_blobs::api::downloader::ContentDiscovery
#[derive(Debug, Clone)]
pub struct DhtContentDiscovery {
    api: api::ApiClient,
}

impl DhtContentDiscovery {
    /// Create a new content discovery backed by the given DHT API client.
    pub fn new(api: api::ApiClient) -> Self {
        Self { api }
    }
}

impl iroh_blobs::api::downloader::ContentDiscovery for DhtContentDiscovery {
    fn find_providers(
        &self,
        hash: iroh_blobs::HashAndFormat,
    ) -> n0_future::stream::Boxed<iroh_base::EndpointId> {
        let api = self.api.clone();
        let blake3_hash = blake3::Hash::from_bytes(*hash.hash.as_bytes());
        Box::pin(n0_future::stream::once_future(async move {
            match api.find_providers(blake3_hash).await {
                Ok(providers) => n0_future::stream::iter(providers),
                Err(_) => n0_future::stream::iter(vec![]),
            }
        }).flatten())
    }
}
