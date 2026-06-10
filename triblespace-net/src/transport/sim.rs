//! Deterministic in-memory transport for simulation testing.
//!
//! [`SimNet`] is a process-local network: nodes join it, get a
//! [`Harness<SimTransport>`] back, and from there the *entire*
//! production protocol stack — host loop, OP_AUTH, fetch_reachable,
//! cap delivery, gossip head tracking — runs unmodified over
//! in-memory pipes instead of iroh QUIC.
//!
//! # Determinism contract
//!
//! A simulated execution is a pure function of `(seed, scenario)`
//! provided the harness follows the rules:
//!
//! 1. **One thread.** Everything runs on a single
//!    `current_thread` tokio runtime built with `.start_paused(true)`.
//!    No cross-thread races exist because there is no second thread.
//! 2. **Virtual time only.** Install a [`crate::clock::VirtualClock`]
//!    before the first time read, and advance it in lockstep with
//!    `tokio::time::advance` via [`SimNet::step`]. Time moves only
//!    when the scenario script says so; every latency sleep and
//!    cooldown check resolves in deterministic order on the paused
//!    timer wheel.
//! 3. **Seeded randomness.** Link latencies and drops draw from the
//!    net's own seeded RNG; protocol-side id minting is seeded via
//!    `triblespace_core::id::rngid::seed_ids` (the `deterministic`
//!    feature this module's `sim` feature pulls in). Node keys are
//!    derived from the seed by the test harness.
//!
//! # Fault injection
//!
//! [`SimNet::partition`] / [`SimNet::heal`] block dialing and gossip
//! between pairs; [`SimNet::crash`] takes a node off the network
//! entirely (dials fail, gossip skips it) until
//! [`SimNet::revive`]. Per-frame gossip drops happen with
//! [`SimConfig::gossip_drop_prob`]. Faults affect *delivery*, never
//! identity — `Conn::remote_id` always reports the true dialer, so
//! identity-dependent protocol logic (dialer-equals-issuer, OP_AUTH
//! subject binding) is exercised honestly.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use super::{Alpn, Conn, GossipEvent, GossipSink, Harness, Incoming, PeerId, Transport};

/// Capacity of each in-memory stream pipe. Generous enough that
/// protocol frames never deadlock on backpressure (max blob size is
/// enforced above the seam at 1 MiB; chain blobs are tiny).
const PIPE_CAPACITY: usize = 4 * 1024 * 1024;

/// Tunables for the simulated network.
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// Per-message one-way latency, drawn uniformly per delivery.
    pub latency: Range<Duration>,
    /// Probability of silently dropping a gossip frame on a given
    /// link (connection-oriented traffic is never dropped — QUIC
    /// would retransmit; model connection loss with
    /// [`SimNet::partition`] / [`SimNet::crash`] instead).
    pub gossip_drop_prob: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            latency: Duration::from_millis(1)..Duration::from_millis(30),
            gossip_drop_prob: 0.0,
        }
    }
}

struct NodeSlot {
    incoming_tx: mpsc::UnboundedSender<Incoming<SimConn>>,
    gossip_tx: Option<mpsc::UnboundedSender<GossipEvent>>,
    up: bool,
}

struct SimNetInner {
    nodes: BTreeMap<PeerId, NodeSlot>,
    /// Symmetric partition set; (a, b) stored with a <= b.
    partitions: BTreeSet<(PeerId, PeerId)>,
    /// Content-discovery table: hash -> providers, in deterministic
    /// (BTree) order.
    dht: BTreeMap<[u8; 32], BTreeSet<PeerId>>,
    rng: StdRng,
    config: SimConfig,
}

impl SimNetInner {
    fn latency(&mut self) -> Duration {
        let lo = self.config.latency.start;
        let hi = self.config.latency.end;
        if hi <= lo {
            return lo;
        }
        let span = (hi - lo).as_nanos() as u64;
        lo + Duration::from_nanos(self.rng.gen_range(0..span))
    }

    fn partitioned(&self, a: &PeerId, b: &PeerId) -> bool {
        let key = if a <= b { (*a, *b) } else { (*b, *a) };
        self.partitions.contains(&key)
    }
}

/// The simulated network. Cheap to clone (Arc).
#[derive(Clone)]
pub struct SimNet {
    inner: Arc<Mutex<SimNetInner>>,
}

impl SimNet {
    /// A fresh network with seeded link randomness.
    pub fn new(seed: u64, config: SimConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimNetInner {
                nodes: BTreeMap::new(),
                partitions: BTreeSet::new(),
                dht: BTreeMap::new(),
                rng: StdRng::seed_from_u64(seed),
                config,
            })),
        }
    }

    /// Join the network as `id`. Returns the transport harness for
    /// the node's host loop. `gossip` controls whether the node
    /// participates in the team topic (mirrors `PeerConfig::gossip`).
    ///
    /// Joining emits `NeighborUp` both ways between the new node and
    /// every existing gossip participant — the sim mesh is fully
    /// connected, which makes `delivered_from` always the original
    /// publisher (a simplification PlumTree converges to for small
    /// meshes anyway).
    pub fn join(&self, id: PeerId, gossip: bool) -> Harness<SimTransport> {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let gossip_pair = if gossip {
            Some(mpsc::unbounded_channel())
        } else {
            None
        };

        let mut inner = self.inner.lock().unwrap();
        if let Some((new_tx, _)) = &gossip_pair {
            for (other_id, other) in inner.nodes.iter() {
                if let Some(other_tx) = &other.gossip_tx {
                    let _ = other_tx.send(GossipEvent::NeighborUp(id));
                    let _ = new_tx.send(GossipEvent::NeighborUp(*other_id));
                }
            }
        }
        inner.nodes.insert(
            id,
            NodeSlot {
                incoming_tx,
                gossip_tx: gossip_pair.as_ref().map(|(tx, _)| tx.clone()),
                up: true,
            },
        );
        drop(inner);

        let transport = SimTransport {
            net: self.clone(),
            id,
        };
        // The bounded receivers the host loop expects: bridge from our
        // unbounded internals. (Unbounded internally so fault-time
        // sends never block the simulator's lock scope.)
        let (b_incoming_tx, b_incoming_rx) = mpsc::channel(1024);
        tokio::spawn(bridge(incoming_rx, b_incoming_tx));
        let gossip = gossip_pair.map(|(tx, rx)| {
            let (b_tx, b_rx) = mpsc::channel(1024);
            tokio::spawn(bridge(rx, b_tx));
            (
                SimGossip {
                    net: self.clone(),
                    from: id,
                    _tx: tx,
                },
                b_rx,
            )
        });
        Harness {
            transport,
            incoming: b_incoming_rx,
            gossip,
        }
    }

    /// Sever the link between `a` and `b` (both directions): dials
    /// fail, gossip frames stop flowing. Existing in-flight pipes
    /// keep draining — like a real partition, packets already in the
    /// kernel buffer arrive.
    pub fn partition(&self, a: PeerId, b: PeerId) {
        let key = if a <= b { (a, b) } else { (b, a) };
        self.inner.lock().unwrap().partitions.insert(key);
    }

    /// Restore the link between `a` and `b`.
    pub fn heal(&self, a: PeerId, b: PeerId) {
        let key = if a <= b { (a, b) } else { (b, a) };
        self.inner.lock().unwrap().partitions.remove(&key);
    }

    /// Take `id` off the network: dials to it fail, gossip skips it.
    /// Its host loop keeps running (a crashed process is modeled by
    /// also dropping the node's Peer + harness; a *disconnected* node
    /// is modeled by this alone).
    pub fn crash(&self, id: PeerId) {
        if let Some(n) = self.inner.lock().unwrap().nodes.get_mut(&id) {
            n.up = false;
        }
    }

    /// Bring `id` back onto the network.
    pub fn revive(&self, id: PeerId) {
        if let Some(n) = self.inner.lock().unwrap().nodes.get_mut(&id) {
            n.up = true;
        }
    }

    /// Advance the simulation by `dur`: moves the virtual clock and
    /// the paused tokio timer wheel together, then yields enough
    /// times for woken tasks to run to their next await point.
    ///
    /// This is the discrete-event scheduler's tick. Requires the
    /// caller to be inside a `start_paused(true)` current-thread
    /// runtime with `clock` installed virtual.
    pub async fn step(clock: &crate::clock::VirtualClock, dur: Duration) {
        clock.advance(dur);
        tokio::time::advance(dur).await;
        // Let everything woken by the timer wheel run. A handful of
        // yields drains multi-hop cascades (timer → task → channel →
        // task); the budget is generous because yields are free.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }
}

/// Forward from the unbounded internal channel to the bounded one the
/// harness exposes.
async fn bridge<T: Send + 'static>(
    mut rx: mpsc::UnboundedReceiver<T>,
    tx: mpsc::Sender<T>,
) {
    while let Some(item) = rx.recv().await {
        if tx.send(item).await.is_err() {
            return;
        }
    }
}

/// One node's capability handle onto the [`SimNet`].
#[derive(Clone)]
pub struct SimTransport {
    net: SimNet,
    id: PeerId,
}

impl Transport for SimTransport {
    type Conn = SimConn;
    type Gossip = SimGossip;

    fn local_id(&self) -> PeerId {
        self.id
    }

    async fn dial(&self, peer: PeerId, alpn: Alpn) -> anyhow::Result<Self::Conn> {
        let (latency, incoming_tx) = {
            let mut inner = self.net.inner.lock().unwrap();
            if inner.partitioned(&self.id, &peer) {
                anyhow::bail!("simnet: {} -> {}: partitioned",
                    hex_prefix(&self.id), hex_prefix(&peer));
            }
            let incoming_tx = {
                let Some(slot) = inner.nodes.get(&peer) else {
                    anyhow::bail!("simnet: dial {}: unknown node", hex_prefix(&peer));
                };
                if !slot.up {
                    anyhow::bail!("simnet: dial {}: node down", hex_prefix(&peer));
                }
                let me = inner.nodes.get(&self.id);
                if me.map(|m| !m.up).unwrap_or(true) {
                    anyhow::bail!("simnet: dial from downed node {}", hex_prefix(&self.id));
                }
                slot.incoming_tx.clone()
            };
            (inner.latency(), incoming_tx)
        };

        // Connection setup costs one round trip.
        tokio::time::sleep(latency * 2).await;

        let (dialer, acceptor) = SimConn::pair(self.id, peer);
        incoming_tx
            .send(Incoming {
                alpn,
                conn: acceptor,
            })
            .map_err(|_| anyhow::anyhow!("simnet: dial {}: node gone", hex_prefix(&peer)))?;
        Ok(dialer)
    }

    async fn dht_announce(&self, hash: [u8; 32]) {
        let mut inner = self.net.inner.lock().unwrap();
        inner.dht.entry(hash).or_default().insert(self.id);
    }

    async fn dht_providers(&self, hash: [u8; 32]) -> Vec<PeerId> {
        let latency = {
            let mut inner = self.net.inner.lock().unwrap();
            inner.latency()
        };
        tokio::time::sleep(latency).await;
        let inner = self.net.inner.lock().unwrap();
        inner
            .dht
            .get(&hash)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// Broadcast half of the sim gossip topic.
#[derive(Clone)]
pub struct SimGossip {
    net: SimNet,
    from: PeerId,
    /// Keeps the node's own event channel alive for the lifetime of
    /// the sink (mirrors iroh-gossip, where dropping the topic handle
    /// ends the subscription).
    _tx: mpsc::UnboundedSender<GossipEvent>,
}

impl GossipSink for SimGossip {
    async fn broadcast(&self, frame: Vec<u8>) -> anyhow::Result<()> {
        // Collect targets + per-target latency under the lock, then
        // deliver outside it via delayed tasks on the paused wheel.
        let deliveries: Vec<(mpsc::UnboundedSender<GossipEvent>, Duration)> = {
            let mut inner = self.net.inner.lock().unwrap();
            let me_up = inner
                .nodes
                .get(&self.from)
                .map(|n| n.up)
                .unwrap_or(false);
            if !me_up {
                return Ok(()); // crashed nodes shout into the void
            }
            let targets: Vec<PeerId> = inner
                .nodes
                .iter()
                .filter(|(id, slot)| {
                    **id != self.from && slot.up && slot.gossip_tx.is_some()
                })
                .map(|(id, _)| *id)
                .collect();
            targets
                .into_iter()
                .filter_map(|id| {
                    if inner.partitioned(&self.from, &id) {
                        return None;
                    }
                    let drop_prob = inner.config.gossip_drop_prob;
                    if drop_prob > 0.0 && inner.rng.gen_bool(drop_prob) {
                        return None;
                    }
                    let lat = inner.latency();
                    inner
                        .nodes
                        .get(&id)
                        .and_then(|n| n.gossip_tx.clone())
                        .map(|tx| (tx, lat))
                })
                .collect()
        };

        let from = self.from;
        for (tx, lat) in deliveries {
            let bytes = frame.clone();
            tokio::spawn(async move {
                tokio::time::sleep(lat).await;
                let _ = tx.send(GossipEvent::Received {
                    bytes,
                    delivered_from: from,
                });
            });
        }
        Ok(())
    }
}

/// A simulated connection: two endpoints exchanging bidirectional
/// streams over in-memory pipes.
#[derive(Clone)]
pub struct SimConn {
    local: PeerId,
    remote: PeerId,
    /// Streams we open land on the remote's accept queue.
    open_tx: mpsc::UnboundedSender<(DuplexStream, DuplexStream)>,
    /// Streams the remote opens land here.
    accept_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(DuplexStream, DuplexStream)>>>,
    /// Shared close flag — either end closing kills both directions.
    closed: Arc<AtomicBool>,
    notify_close: Arc<tokio::sync::Notify>,
}

impl SimConn {
    fn pair(dialer: PeerId, acceptor: PeerId) -> (SimConn, SimConn) {
        let (d2a_tx, d2a_rx) = mpsc::unbounded_channel();
        let (a2d_tx, a2d_rx) = mpsc::unbounded_channel();
        let closed = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());
        let dialer_end = SimConn {
            local: dialer,
            remote: acceptor,
            open_tx: d2a_tx,
            accept_rx: Arc::new(tokio::sync::Mutex::new(a2d_rx)),
            closed: closed.clone(),
            notify_close: notify.clone(),
        };
        let acceptor_end = SimConn {
            local: acceptor,
            remote: dialer,
            open_tx: a2d_tx,
            accept_rx: Arc::new(tokio::sync::Mutex::new(d2a_rx)),
            closed,
            notify_close: notify,
        };
        (dialer_end, acceptor_end)
    }
}

impl Conn for SimConn {
    type SendHalf = DuplexStream;
    type RecvHalf = DuplexStream;

    fn remote_id(&self) -> PeerId {
        self.remote
    }

    async fn open_bi(&self) -> anyhow::Result<(DuplexStream, DuplexStream)> {
        if self.closed.load(Ordering::SeqCst) {
            anyhow::bail!(
                "simnet: open_bi on closed conn {} -> {}",
                hex_prefix(&self.local),
                hex_prefix(&self.remote)
            );
        }
        // Two pipes per bi-stream: one per direction. Each duplex()
        // call returns a connected pair; we use one side for writing
        // and hand the other to the remote for reading (and vice
        // versa).
        let (local_send, remote_recv) = tokio::io::duplex(PIPE_CAPACITY);
        let (remote_send, local_recv) = tokio::io::duplex(PIPE_CAPACITY);
        self.open_tx
            .send((remote_send, remote_recv))
            .map_err(|_| anyhow::anyhow!("simnet: open_bi: remote end dropped"))?;
        Ok((local_send, local_recv))
    }

    async fn accept_bi(&self) -> Option<(DuplexStream, DuplexStream)> {
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }
        let mut rx = self.accept_rx.lock().await;
        tokio::select! {
            stream = rx.recv() => stream,
            _ = self.notify_close.notified() => None,
        }
    }

    fn close(&self, _code: u32, _reason: &[u8]) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify_close.notify_waiters();
    }
}

fn hex_prefix(id: &PeerId) -> String {
    hex::encode(&id[..4])
}
