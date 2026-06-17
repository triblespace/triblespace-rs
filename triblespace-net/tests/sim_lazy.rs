//! Lazy-replication read path — deterministic simulation.
//!
//! Exercises the swarm-addressed on-demand fetch
//! (`Peer::request_blob` / `NetCommand::FetchBlob`) and, as the build
//! progresses, the `PeerReader` fall-through and pin policies. The
//! property under test: a node which does NOT hold a content blob can
//! still obtain it from whoever in the swarm does — without every node
//! eagerly replicating everything.
//!
//! Sim note: the swarm fetch replies on a tokio oneshot. Tests either
//! drive `request_blob` + step the sim while polling the reply
//! (`try_recv`) — the steppable idiom — or, for the async read
//! (`get_or_fetch_async`), poll the future and step on `Pending` so the
//! host produces the reply between polls. No thread is ever blocked.
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

/// Drive the sim until `rx` produces a reply or the step budget runs
/// out. Returns the reply (`Some(bytes)` / `None`) or `None` if the
/// budget exhausted.
async fn drive_until<F: FnMut()>(
    rx: &mut tokio::sync::oneshot::Receiver<Option<Vec<u8>>>,
    mut on_step: F,
    steps: u32,
) -> Option<Vec<u8>> {
    use tokio::sync::oneshot::error::TryRecvError;
    for _ in 0..steps {
        SimNet::step(&vclock(), Duration::from_millis(20)).await;
        on_step();
        match rx.try_recv() {
            Ok(reply) => return reply,
            Err(TryRecvError::Empty) => continue,
            Err(TryRecvError::Closed) => return None,
        }
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

        let mut rx = peer_b.request_blob(hash);
        let got = drive_until(&mut rx, || peer_a.refresh(), 120)
            .await
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
        let mut rx = peer_b.request_blob(hash);
        let got = drive_until(&mut rx, || peer_a.refresh(), 120)
            .await
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
            let mut rx = peer_b.request_blob(*hash);
            let got = drive_until(&mut rx, || peer_a.refresh(), 120)
                .await
                .expect("swarm must serve each blob");
            peer_b.land_in_cache(got.into());
        }

        // Capacity held: the first (oldest) evicted, the last two resident.
        assert_eq!(peer_b.cache_len(), 2, "cache bounded to capacity");
        assert!(peer_b.try_local(blobs[0].1).is_none(), "oldest evicted from the union");
        assert!(peer_b.try_local(blobs[1].1).is_some(), "second still cached");
        assert!(peer_b.try_local(blobs[2].1).is_some(), "newest still cached");

        // The evicted blob is re-fetchable from the swarm.
        let mut rx = peer_b.request_blob(blobs[0].1);
        let refetched = drive_until(&mut rx, || peer_a.refresh(), 120)
            .await
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
        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);

        let (_blob, hash) = content_blob(0x99);
        let mut rx = peer_a.request_blob(hash);
        // providers_for has a 3s internal DHT timeout; give the sim
        // enough virtual steps to cross it, then expect a None reply.
        let reply = drive_until(&mut rx, || peer_a.refresh(), 400).await;
        assert!(
            reply.is_none(),
            "unavailable fetch must resolve to None, got {:?} bytes",
            reply.map(|b| b.len())
        );
    });
}
