//! Lazy-replication read path — deterministic simulation.
//!
//! Exercises the swarm-addressed on-demand fetch (`Peer::fetch_blob`,
//! run inline via `host::NetCapability`) plus the `PeerReader`
//! fall-through and the transparent async read. The property under
//! test: a node which does NOT hold a content blob can still obtain it
//! from whoever in the swarm does — without every node eagerly
//! replicating everything.
//!
//! Sim note: the fetch runs inline (a future to poll), so tests drive it
//! with `drive_future` — poll the future, and on `Pending` step the sim
//! so the host (and the fetch) make progress between polls. No thread is
//! ever blocked and nothing rides a reply channel.
#![cfg(feature = "sim")]

mod common;

use std::time::Duration;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::Blob;
use triblespace_core::blob::IntoBlob;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::async_store::AsyncBlobStoreGet;
use triblespace_core::repo::{BlobStoreGet, BlobStorePut};
use triblespace_core::trible::TribleSet;
use triblespace_net::transport::sim::{DhtMode, SimConfig, SimNet};

use common::*;

/// A throwaway content blob (a tiny SimpleArchive) + its hash. Stands
/// in for a "content payload" that lives outside the eagerly-replicated
/// history.
fn content_blob(tag_byte: u8) -> (Blob<SimpleArchive>, [u8; 32]) {
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::id::Id;
    use triblespace_core::macros::entity;
    let e = Id::new([tag_byte; 16]).expect("nonzero id");
    let ts: TribleSet = entity! {
        ExclusiveId::force_ref(&e) @
        triblespace_core::metadata::tag: Id::new([tag_byte.wrapping_add(1).max(1); 16]).unwrap(),
    }
    .into();
    let blob: Blob<SimpleArchive> = ts.to_blob();
    let hash = blob.get_handle().raw;
    (blob, hash)
}

/// Drive `fut` to completion, stepping the sim between polls so the
/// host loop and the *inline* swarm fetch make progress. Returns
/// `Some(value)`, or `None` if the step budget is exhausted. This is the
/// deterministic-sim idiom now that the fetch runs inline (a future to
/// poll) rather than replying on a channel to drain.
async fn drive_future<T, Fut, F>(fut: Fut, mut on_step: F, steps: u32) -> Option<T>
where
    Fut: std::future::Future<Output = T>,
    F: FnMut(),
{
    let mut fut = Box::pin(fut);
    for _ in 0..steps {
        if let std::task::Poll::Ready(v) = futures::poll!(fut.as_mut()) {
            return Some(v);
        }
        SimNet::step(&vclock(), Duration::from_millis(20)).await;
        on_step();
    }
    None
}

fn holds_locally(peer: &mut triblespace_net::peer::Peer<triblespace_core::repo::memoryrepo::MemoryRepo>, hash: [u8; 32]) -> bool {
    let reader = peer.reader().unwrap();
    // Disambiguate: the sync, local-only `BlobStoreGet::get` (PeerReader
    // also impls the async fetching `AsyncBlobStoreGet::get`).
    BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(hash)).is_ok()
}

/// A holds a content blob; B does not. B's swarm fetch must pull it
/// from A (DHT-resolved) and return the verified bytes.
#[test]
fn fetch_blob_pulls_from_the_holder() {
    let _g = sim_guard();
    run_paused(0xABCD, async {
        let net = SimNet::new(0xABCD, SimConfig::default());
        let root = key(0xF0);
        let ka = key(0xA0);
        let kb = key(0xB0);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x42);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b = bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        // A announces its blobs to the DHT via refresh's diff-and-announce;
        // settle the mesh (neighbor-up + announce).
        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        assert!(!holds_locally(&mut peer_b, hash), "precondition: B lacks the blob");

        let got = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 120)
            .await
            .flatten()
            .expect("B must obtain the blob from the swarm");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "fetched bytes must hash to the requested content id"
        );
    });
}

/// The full lazy-read invariant: a cached node B that does not hold a
/// content blob fetches it from the swarm and lands it in its **Cache**
/// tier — never the Durable store — after which the Durable∪Cache
/// `PeerReader` serves it locally. This is "lazy replication" in one
/// test: B reads content it never eagerly replicated, holds it only
/// transiently, and a pin would be a separate decision.
#[test]
fn lazy_read_lands_in_cache_not_durable() {
    let _g = sim_guard();
    run_paused(0xCAFE, async {
        let net = SimNet::new(0xCAFE, SimConfig::default());
        let root = key(0xF2);
        let ka = key(0xA2);
        let kb = key(0xB2);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x55);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        // B is a lazy node: a small bounded cache, no eager content.
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        // Settle the mesh so A's blobs are announced to the DHT.
        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Precondition: B holds nothing locally and its cache is empty.
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");
        assert_eq!(peer_b.cache_len(), 0, "precondition: B's cache is empty");

        // Swarm-fetch (steppable form of get_or_fetch's miss path).
        let got = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 120)
            .await
            .flatten()
            .expect("B must obtain the blob from the swarm");

        // Land it exactly as get_or_fetch would.
        peer_b.land_in_cache(got.clone().into());

        // 1. The Durable∪Cache reader now serves it locally.
        let local = peer_b.try_local(hash).expect("PeerReader union serves the cached blob");
        assert_eq!(
            blake3::hash(&local).as_bytes(),
            &hash,
            "served bytes hash to the content id"
        );
        // 2. It landed in the Cache tier...
        assert_eq!(peer_b.cache_len(), 1, "blob resident in the cache tier");
        // 3. ...and NOT in the Durable store (no eager persistence).
        let durable_has = peer_b
            .store_mut()
            .reader()
            .unwrap()
            .get::<Blob<UnknownBlob>, UnknownBlob>(Inline::new(hash))
            .is_ok();
        assert!(!durable_has, "lazy read must NOT persist to the Durable store");
    });
}

/// A bounded cache evicts under read pressure, and the evicted blob is
/// re-fetchable. B caches at most 2 blobs; after lazily reading 3, the
/// oldest is gone from the local union (a miss) but the swarm still
/// serves it on demand — "caches are free, eviction is always safe".
#[test]
fn lazy_cache_evicts_under_pressure_and_refetches() {
    let _g = sim_guard();
    run_paused(0xBEEF, async {
        let net = SimNet::new(0xBEEF, SimConfig::default());
        let root = key(0xF3);
        let ka = key(0xA3);
        let kb = key(0xB3);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        // A holds three content blobs; B caches at most two.
        let blobs: Vec<(Blob<SimpleArchive>, [u8; 32])> =
            (0..3u8).map(|i| content_blob(0x60 + i)).collect();
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        for (b, _) in &blobs {
            store_a.put::<SimpleArchive, _>(b.clone()).unwrap();
        }
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 2, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Lazily read all three, in order.
        for (_, hash) in &blobs {
            let got = drive_future(peer_b.fetch_blob(*hash), || peer_a.refresh(), 120)
                .await
                .flatten()
                .expect("swarm must serve each blob");
            peer_b.land_in_cache(got.into());
        }

        // Capacity held: the first (oldest) evicted, the last two resident.
        assert_eq!(peer_b.cache_len(), 2, "cache bounded to capacity");
        assert!(peer_b.try_local(blobs[0].1).is_none(), "oldest evicted from the union");
        assert!(peer_b.try_local(blobs[1].1).is_some(), "second still cached");
        assert!(peer_b.try_local(blobs[2].1).is_some(), "newest still cached");

        // The evicted blob is re-fetchable from the swarm.
        let refetched = drive_future(peer_b.fetch_blob(blobs[0].1), || peer_a.refresh(), 120)
            .await
            .flatten()
            .expect("evicted blob re-fetchable — caches are free");
        assert_eq!(blake3::hash(&refetched).as_bytes(), &blobs[0].1);
    });
}

/// The honest **async** lazy read: `get_or_fetch_async` awaits the
/// swarm fetch (oneshot reply, no blocked thread) and lands the result
/// in Cache. Driven deterministically by polling the future and
/// stepping the sim on `Pending` — the awaited oneshot resolves once
/// the host (driven by the stepping) sends the reply.
#[test]
fn async_lazy_read_awaits_swarm_and_caches() {
    let _g = sim_guard();
    run_paused(0xA5A5, async {
        let net = SimNet::new(0xA5A5, SimConfig::default());
        let root = key(0xF4);
        let ka = key(0xA4);
        let kb = key(0xB4);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x77);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        // Drive the async read: poll once, and on Pending step the sim so
        // the host can serve the reply. The future holds `&mut peer_b`
        // for its lifetime, so only `peer_a` is touched inside the loop.
        let got = {
            let mut fut = Box::pin(peer_b.get_or_fetch_async(hash));
            loop {
                match futures::poll!(fut.as_mut()) {
                    std::task::Poll::Ready(r) => break r,
                    std::task::Poll::Pending => {
                        SimNet::step(&vclock(), Duration::from_millis(20)).await;
                        peer_a.refresh();
                    }
                }
            }
        };

        let got = got.expect("async lazy read must obtain the blob from the swarm");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "awaited bytes hash to the content id"
        );
        // Landed in Cache, served locally on the next read.
        assert!(peer_b.try_local(hash).is_some(), "now resident in the local union");
        assert_eq!(peer_b.cache_len(), 1, "landed in the cache tier");
    });
}

/// Transparent async read through the trait surface: a *generic*
/// `AsyncBlobStoreGet` consumer calls `reader.get(handle).await` on a
/// blob B doesn't hold, and the `PeerReader` fetches it from the swarm
/// and lands it in the shared Cache — no knowledge that it's a `Peer`.
/// This is the "lazy replication for free" payoff of increment 5b.
#[test]
fn transparent_async_get_fetches_through_reader() {
    let _g = sim_guard();
    run_paused(0x9001, async {
        let net = SimNet::new(0x9001, SimConfig::default());
        let root = key(0xF5);
        let ka = key(0xA5);
        let kb = key(0xB5);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x88);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        // A generic async reader: it only knows `AsyncBlobStoreGet`.
        let got: anybytes::Bytes = {
            let reader = peer_b.reader().unwrap();
            let mut fut = Box::pin(AsyncBlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(
                &reader,
                Inline::new(hash),
            ));
            loop {
                match futures::poll!(fut.as_mut()) {
                    std::task::Poll::Ready(r) => break r,
                    std::task::Poll::Pending => {
                        SimNet::step(&vclock(), Duration::from_millis(20)).await;
                        peer_a.refresh();
                    }
                }
            }
            .expect("transparent get must fetch the blob from the swarm")
        };

        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "transparently-fetched bytes hash to the content id"
        );
        // The fetch landed in the *shared* cache (a &self read mutated
        // Peer state), so a fresh local read now hits.
        assert_eq!(peer_b.cache_len(), 1, "fetch landed in the shared cache tier");
        assert!(peer_b.try_local(hash).is_some(), "served locally on the next read");
    });
}

/// Black-hole DHT, no other holder: the fetch resolves to `None`
/// (Unavailable) — it must complete, not hang.
#[test]
fn fetch_blob_unavailable_is_clean() {
    let _g = sim_guard();
    run_paused(0x1234, async {
        let net = SimNet::new(
            0x1234,
            SimConfig {
                dht: DhtMode::Blackhole,
                ..SimConfig::default()
            },
        );
        let root = key(0xF1);
        let ka = key(0xA1);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);

        let store_a = store_with_caps(&[cap_a.clone()]);
        let peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);

        let (_blob, hash) = content_blob(0x99);
        // providers_for has a 3s internal DHT timeout; give the sim
        // enough virtual steps to cross it, then expect a None reply.
        // No-op on_step: the inline fetch borrows `peer_a`, and stepping
        // alone advances virtual time across the timeout (no refresh
        // needed — there's nothing to gossip to here anyway).
        let reply = drive_future(peer_a.fetch_blob(hash), || {}, 400)
            .await
            .flatten();
        assert!(
            reply.is_none(),
            "unavailable fetch must resolve to None, got {:?} bytes",
            reply.map(|b| b.len())
        );
    });
}

/// Lazy read degrades to Unavailable when the network partitions the
/// reader from the only holder, and **recovers** once the link heals —
/// the graceful-degradation property under a real fault. (The DHT find
/// still names the holder; it's the dial that fails, then succeeds.)
#[test]
fn lazy_read_unavailable_under_partition_then_heals() {
    let _g = sim_guard();
    run_paused(0xD15C, async {
        let net = SimNet::new(0xD15C, SimConfig::default());
        let root = key(0xF6);
        let ka = key(0xA6);
        let kb = key(0xB6);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0xC1);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Sever A↔B: the DHT still resolves A as the provider, but B's
        // dial to A fails.
        net.partition(pk(&ka), pk(&kb));
        let blocked = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 300)
            .await
            .flatten();
        assert!(
            blocked.is_none(),
            "partitioned from the only holder → Unavailable"
        );
        assert_eq!(peer_b.cache_len(), 0, "nothing cached from a failed fetch");

        // Heal the link; the same read now succeeds.
        net.heal(pk(&ka), pk(&kb));
        let got = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 300)
            .await
            .flatten()
            .expect("after heal the holder is reachable again");
        assert_eq!(blake3::hash(&got).as_bytes(), &hash);
    });
}

/// Same graceful-degradation property under a node **crash** rather than
/// a link partition: the holder crashing makes the read Unavailable
/// (its connections reset, re-dials fail), and reviving it restores
/// service. Exercises the conn-pool's evict-on-error + re-dial path.
#[test]
fn lazy_read_unavailable_under_crash_then_revives() {
    let _g = sim_guard();
    run_paused(0xC1A5, async {
        let net = SimNet::new(0xC1A5, SimConfig::default());
        let root = key(0xF7);
        let ka = key(0xA7);
        let kb = key(0xB7);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0xC2);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        net.crash(pk(&ka));
        let blocked = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 300)
            .await
            .flatten();
        assert!(blocked.is_none(), "holder crashed → Unavailable");

        net.revive(pk(&ka));
        let got = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 300)
            .await
            .flatten()
            .expect("after revive the holder serves again");
        assert_eq!(blake3::hash(&got).as_bytes(), &hash);
    });
}

/// A default `Peer<S, NullCache>` (no cache tier — what `Peer::new`
/// gives) still fetches lazily: the fetch succeeds and returns the
/// bytes, but the no-op `NullCache` land caches nothing, so a second
/// read re-fetches rather than hitting locally. Validates the
/// cache-less config through the lazy machinery — the `SharedCache<
/// NullCache>` sink path that the bounded-cache tests never exercise.
#[test]
fn nullcache_peer_fetches_but_caches_nothing() {
    let _g = sim_guard();
    run_paused(0x0011_0000, async {
        let net = SimNet::new(0x0011_0000, SimConfig::default());
        let root = key(0xFC);
        let ka = key(0xAC);
        let kb = key(0xBC);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0xCD);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        // bring_up gives a plain `Peer<MemoryRepo, NullCache>` — no cache.
        let mut peer_b = bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        let got = drive_future(peer_b.get_or_fetch_async(hash), || peer_a.refresh(), 200)
            .await
            .flatten()
            .expect("a cache-less peer still fetches from the swarm");
        assert_eq!(blake3::hash(&got).as_bytes(), &hash);

        // NullCache dropped the land — nothing is cached, nothing local.
        assert_eq!(peer_b.cache_len(), 0, "NullCache caches nothing");
        assert!(peer_b.try_local(hash).is_none(), "still a local miss after the fetch");

        // A second read re-fetches and still succeeds.
        let again = drive_future(peer_b.get_or_fetch_async(hash), || peer_a.refresh(), 200)
            .await
            .flatten()
            .expect("re-fetch succeeds (no local hit to short-circuit)");
        assert_eq!(blake3::hash(&again).as_bytes(), &hash);
    });
}

/// Randomized fault **chaos** — the Jepsen-style property fixed
/// scenarios miss. Across several seeds, the A↔B link is partitioned and
/// healed at random steps while B retries its lazy read; the back half
/// of each run is forced healthy. Two invariants:
///   * SAFETY — any bytes the fetch returns hash to the requested
///     content id. Chaos never yields corrupt data.
///   * LIVENESS — once the link stops flapping and stays healed, the
///     read eventually succeeds.
#[test]
fn lazy_fetch_under_partition_chaos_is_safe_and_recovers() {
    use rand::{Rng, SeedableRng};
    let _g = sim_guard();
    for s in 0..6u64 {
        let seed = 0x0C4A_0500 + s;
        run_paused(seed, async move {
            let net = SimNet::new(seed, SimConfig::default());
            let root = key(0xFA);
            let ka = key(0xAA);
            let kb = key(0xBA);
            let team_root = root.verifying_key();
            let cap_a = admin_cap(&root, &ka);
            let cap_b = admin_cap(&root, &kb);

            let (blob, hash) = content_blob(0xAB);
            let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
            store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
            let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

            let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
            let peer_b =
                bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

            for _ in 0..40u32 {
                SimNet::step(&vclock(), Duration::from_millis(20)).await;
                peer_a.refresh();
            }

            let pa = pk(&ka);
            let pb = pk(&kb);
            let mut frng = rand::rngs::StdRng::seed_from_u64(seed ^ 0xF417);
            const FLAP_UNTIL: u32 = 250;
            const BUDGET: u32 = 600;

            let mut got: Option<Vec<u8>> = None;
            let mut fut = Box::pin(peer_b.fetch_blob(hash));
            for step in 0..BUDGET {
                if let std::task::Poll::Ready(v) = futures::poll!(fut.as_mut()) {
                    if let Some(bytes) = v {
                        // SAFETY invariant.
                        assert_eq!(
                            blake3::hash(&bytes).as_bytes(),
                            &hash,
                            "chaos must never yield corrupt bytes (seed {seed:#x})"
                        );
                        got = Some(bytes);
                        break;
                    }
                    // One-shot attempt failed (partitioned mid-fetch);
                    // retry. The old future drops here, freeing its
                    // shared borrow of peer_b.
                    fut = Box::pin(peer_b.fetch_blob(hash));
                }

                if step < FLAP_UNTIL {
                    if frng.gen_bool(0.12) {
                        if frng.gen_bool(0.5) {
                            net.partition(pa, pb);
                        } else {
                            net.heal(pa, pb);
                        }
                    }
                } else if step == FLAP_UNTIL {
                    net.heal(pa, pb); // hold healthy so liveness can assert
                }

                SimNet::step(&vclock(), Duration::from_millis(20)).await;
                peer_a.refresh();
            }

            // LIVENESS invariant.
            assert!(
                got.is_some(),
                "lazy read must recover after the partition stops flapping (seed {seed:#x})"
            );
        });
    }
}

/// Provider fallback across a 3-node mesh: the blob lives on both A and
/// C; A crashes; B's lazy read must fall back to the surviving holder.
/// Exercises `fetch_one`'s multi-provider iteration (try next provider
/// on a dial/op failure) — invisible to the 2-node tests, where there's
/// only ever one provider.
#[test]
fn lazy_fetch_falls_back_to_a_second_holder() {
    let _g = sim_guard();
    run_paused(0xFA11, async {
        let net = SimNet::new(0xFA11, SimConfig::default());
        let root = key(0xF9);
        let ka = key(0xA9);
        let kb = key(0xB9);
        let kc = key(0xC9);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);
        let cap_c = admin_cap(&root, &kc);
        let all = [cap_a.clone(), cap_b.clone(), cap_c.clone()];

        let (blob, hash) = content_blob(0xFB);
        // A and C both hold the blob; B does not.
        let mut store_a = store_with_caps(&all);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let mut store_c = store_with_caps(&all);
        store_c.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&all);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_c = bring_up(&net, &kc, store_c, team_root, self_cap_of(&cap_c.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..50u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
            peer_c.refresh();
        }
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        // Crash A. Both A and C are DHT providers; B must fall back to C.
        net.crash(pk(&ka));
        let got = drive_future(
            peer_b.fetch_blob(hash),
            || {
                peer_c.refresh();
            },
            400,
        )
        .await
        .flatten()
        .expect("B must fall back to the surviving holder C");
        assert_eq!(blake3::hash(&got).as_bytes(), &hash);
    });
}

/// Run one full lazy-fetch scenario under `seed` and return the observed
/// outcome: the fetched bytes (if any) and the number of sim steps the
/// fetch took to complete. The step count is latency-sensitive — link
/// latencies are drawn from the seeded net RNG — so it's a real
/// seed-dependent observable, exactly what a determinism check wants.
fn run_lazy_fetch(seed: u64, config: SimConfig) -> (Option<Vec<u8>>, u32) {
    run_paused(seed, async move {
        let net = SimNet::new(seed, config);
        let root = key(0xF0);
        let ka = key(0xA0);
        let kb = key(0xB0);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x42);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Drive the fetch, counting steps until completion.
        let mut fut = Box::pin(peer_b.fetch_blob(hash));
        let mut steps = 0u32;
        let got = loop {
            if let std::task::Poll::Ready(v) = futures::poll!(fut.as_mut()) {
                break v;
            }
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
            steps += 1;
            if steps > 600 {
                break None;
            }
        };
        (got, steps)
    })
}

/// Two concurrent transparent reads on the *same* node for the *same*
/// missing blob. Stresses the 5b shared cache (interior-mutable
/// `Arc<Mutex>`): both `&self` reads fetch from the swarm and land into
/// the one shared tier. The conn-pool singleflight should share the dial
/// to the holder, and the content-addressed cache must end with exactly
/// one copy — no double-store from the racing lands.
#[test]
fn concurrent_transparent_reads_share_cache_and_dedupe() {
    let _g = sim_guard();
    run_paused(0xC0FFEE, async {
        let net = SimNet::new(0xC0FFEE, SimConfig::default());
        let root = key(0xF8);
        let ka = key(0xA8);
        let kb = key(0xB8);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0xCC);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        // Two independent readers off the same Peer — each owns a clone
        // of the durable+cache snapshot and a fetch capability into the
        // *same* shared cache. (reader() borrows &mut only transiently.)
        let reader1 = peer_b.reader().unwrap();
        let reader2 = peer_b.reader().unwrap();

        let (got1, got2) = {
            let mut f1 = Box::pin(AsyncBlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(
                &reader1,
                Inline::new(hash),
            ));
            let mut f2 = Box::pin(AsyncBlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(
                &reader2,
                Inline::new(hash),
            ));
            let mut r1: Option<_> = None;
            let mut r2: Option<_> = None;
            for _ in 0..300u32 {
                if r1.is_none() {
                    if let std::task::Poll::Ready(v) = futures::poll!(f1.as_mut()) {
                        r1 = Some(v);
                    }
                }
                if r2.is_none() {
                    if let std::task::Poll::Ready(v) = futures::poll!(f2.as_mut()) {
                        r2 = Some(v);
                    }
                }
                if r1.is_some() && r2.is_some() {
                    break;
                }
                SimNet::step(&vclock(), Duration::from_millis(20)).await;
                peer_a.refresh();
            }
            (r1, r2)
        };

        let got1 = got1.expect("reader 1 completed").expect("reader 1 fetched");
        let got2 = got2.expect("reader 2 completed").expect("reader 2 fetched");
        assert_eq!(blake3::hash(&got1).as_bytes(), &hash);
        assert_eq!(blake3::hash(&got2).as_bytes(), &hash);
        // Both racing lands hit the same content-addressed cache: one copy.
        assert_eq!(
            peer_b.cache_len(),
            1,
            "concurrent lands of the same blob dedupe to a single cache entry"
        );
    });
}

/// Run a partition → heal → recover lazy fetch under `seed`, returning
/// the recovered bytes and the steps the *recovery* attempt took. The
/// scenario scripts a partition (failed attempt), then a heal, then a
/// timed successful attempt — so the observable folds in both the fault
/// injection and the latency-sensitive recovery.
fn run_lazy_fetch_partition_recovery(seed: u64) -> (Option<Vec<u8>>, u32) {
    run_paused(seed, async move {
        let net = SimNet::new(seed, SimConfig::default());
        let root = key(0xF0);
        let ka = key(0xA0);
        let kb = key(0xB0);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        let (blob, hash) = content_blob(0x42);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let peer_b =
            bring_up_cached(&net, &kb, store_b, 8, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        let pa = pk(&ka);
        let pb = pk(&kb);

        // Partition → a failed attempt → heal.
        net.partition(pa, pb);
        let _ = drive_future(peer_b.fetch_blob(hash), || peer_a.refresh(), 120).await;
        net.heal(pa, pb);

        // Timed recovery attempt.
        let mut fut = Box::pin(peer_b.fetch_blob(hash));
        let mut steps = 0u32;
        let got = loop {
            if let std::task::Poll::Ready(v) = futures::poll!(fut.as_mut()) {
                break v;
            }
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
            steps += 1;
            if steps > 400 {
                break None;
            }
        };
        (got, steps)
    })
}

/// Determinism of the **faulted** path — the property that makes DST
/// bug reports reproducible. Fault injection (partition/heal) and the
/// recovery that follows must be a pure function of the seed too:
/// otherwise a chaos-found failure couldn't be replayed. If `crash`'s
/// conn-retain or `partition`'s set bookkeeping ever leaked
/// non-determinism (HashMap order, wall-clock), the recovery step count
/// would diverge between identical runs.
#[test]
fn faulted_lazy_fetch_is_deterministic() {
    let _g = sim_guard();
    let r1 = run_lazy_fetch_partition_recovery(0x0FD0_0001);
    let r2 = run_lazy_fetch_partition_recovery(0x0FD0_0001);
    assert!(r1.0.is_some(), "sanity: the fetch recovered after heal");
    assert_eq!(
        r1, r2,
        "partition+heal+recovery is reproducible under the same seed"
    );
}

/// The foundational DST guarantee: a simulated run is a **pure function
/// of `(seed, scenario)`**. The identical scenario under the identical
/// seed must produce the identical observable — same fetched bytes *and*
/// same step count. A regression that leaked real wall-clock time, or
/// `HashMap`/`HashSet` iteration order, or any unseeded randomness into
/// the sim would diverge here.
#[test]
fn lazy_fetch_is_deterministic_across_runs() {
    let _g = sim_guard();
    let (bytes1, steps1) = run_lazy_fetch(0x0DDD_0001, SimConfig::default());
    let (bytes2, steps2) = run_lazy_fetch(0x0DDD_0001, SimConfig::default());
    assert!(bytes1.is_some(), "sanity: the fetch actually succeeded");
    assert_eq!(bytes1, bytes2, "same seed → identical fetched bytes");
    assert_eq!(
        steps1, steps2,
        "same seed → identical step count: the sim is a pure function of the seed"
    );
}

/// Liveness property across the seed space: under a healthy network, the
/// lazy read must *always* eventually succeed, whatever the seed-chosen
/// link latencies and id minting. Property-based DST — catches
/// seed-dependent liveness bugs a single hand-picked seed would miss.
#[test]
fn lazy_fetch_succeeds_across_many_seeds() {
    let _g = sim_guard();
    for s in 0..16u64 {
        let seed = 0x5EED_0000 + s;
        let (got, steps) = run_lazy_fetch(seed, SimConfig::default());
        assert!(
            got.is_some(),
            "lazy fetch must succeed under seed {seed:#x} (gave up after {steps} steps)"
        );
    }
}

/// The content layer is decoupled from the gossip layer. Under **total
/// gossip loss** the branch-sync/announce mesh is dark — but the lazy
/// read uses the DHT (a global provider record) plus a direct authed
/// dial, neither of which is gossip, so it must still succeed. A
/// regression that accidentally routed content discovery through gossip
/// would fail here.
#[test]
fn lazy_fetch_is_independent_of_gossip_liveness() {
    let _g = sim_guard();
    let config = SimConfig {
        gossip_drop_prob: 1.0,
        ..SimConfig::default()
    };
    let (got, steps) = run_lazy_fetch(0x6055_1055, config);
    assert!(
        got.is_some(),
        "lazy fetch must succeed despite total gossip loss (gave up after {steps} steps)"
    );
}
