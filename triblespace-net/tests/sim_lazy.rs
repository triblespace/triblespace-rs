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
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::async_store::AsyncBlobStoreGet;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStoreGet, BlobStoreKeep, BlobStoreList, BlobStorePut, PinStore, WeakPinStore,
};
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

fn holds_locally(peer: &mut triblespace_net::peer::Peer<MemoryRepo>, hash: [u8; 32]) -> bool {
    let reader = peer.reader().unwrap();
    // Disambiguate: the sync, local-only `BlobStoreGet::get` (PeerReader
    // also impls the async fetching `AsyncBlobStoreGet::get`).
    BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(hash)).is_ok()
}

/// Count of weak-pinned handles in the peer's store — the retention
/// markers that lazy swarm fetches land under.
fn weak_pin_count(peer: &triblespace_net::peer::Peer<MemoryRepo>) -> usize {
    peer.store().weak_pins().unwrap().count()
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

/// The full lazy-read invariant: a node B that does not hold a content
/// blob fetches it from the swarm and lands it in its store under a
/// **weak pin** — the demand-born retention marker — after which the
/// `PeerReader` serves it locally. This is "lazy replication" in one
/// test: B reads content it never eagerly replicated, retains it as an
/// evictable weak-pinned resident, and a strong pin (the durability
/// promise) would be a separate decision.
#[test]
fn lazy_read_lands_weak_pinned_in_store() {
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
        // B is a lazy node: no eager content.
        let mut peer_b =
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        // Settle the mesh so A's blobs are announced to the DHT.
        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Precondition: B holds nothing locally and has no weak pins.
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");
        assert_eq!(weak_pin_count(&peer_b), 0, "precondition: no weak pins");
        let strong_before = peer_b.store().pins().unwrap().count();

        // The lazy read: record the demand-born weak pin, fetch from
        // the swarm, land the verified bytes in the store.
        let got = drive_future(peer_b.get_or_fetch_async(hash), || peer_a.refresh(), 120)
            .await
            .expect("fetch future completes")
            .expect("want recorded (MemoryRepo pins are infallible)")
            .expect("B must obtain the blob from the swarm");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "fetched bytes hash to the content id"
        );

        // 1. The store now serves it locally.
        let local = peer_b.try_local(hash).expect("the store serves the fetched blob");
        assert_eq!(
            blake3::hash(&local).as_bytes(),
            &hash,
            "served bytes hash to the content id"
        );
        // 2. It is retained under a weak pin (the demand-born marker)...
        let weak: Vec<_> = peer_b.store().weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(
            weak,
            vec![Inline::<Handle<UnknownBlob>>::new(hash)],
            "fetched blob is weak-pinned"
        );
        // 3. ...and NOT strong-pinned (no eager durability promise).
        assert_eq!(
            peer_b.store().pins().unwrap().count(),
            strong_before,
            "lazy read must not create a strong pin"
        );
    });
}

/// Eviction lives in the store now — and it is always safe: the evicted
/// blob is re-fetchable. B lazily reads 3 blobs (each lands weak-pinned),
/// then the store evicts the first (weak-unpin + drop the bytes); the
/// evicted blob becomes a local miss but the swarm still serves it on
/// demand — "weak pins are wants, eviction is always safe".
#[test]
fn lazy_store_eviction_is_safe_and_refetches() {
    let _g = sim_guard();
    run_paused(0xBEEF, async {
        let net = SimNet::new(0xBEEF, SimConfig::default());
        let root = key(0xF3);
        let ka = key(0xA3);
        let kb = key(0xB3);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        // A holds three content blobs; B holds none.
        let blobs: Vec<(Blob<SimpleArchive>, [u8; 32])> =
            (0..3u8).map(|i| content_blob(0x60 + i)).collect();
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        for (b, _) in &blobs {
            store_a.put::<SimpleArchive, _>(b.clone()).unwrap();
        }
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Lazily read all three, in order — each lands weak-pinned.
        for (_, hash) in &blobs {
            let got = drive_future(peer_b.get_or_fetch_async(*hash), || peer_a.refresh(), 120)
                .await
                .expect("fetch future completes")
                .expect("want recorded")
                .expect("swarm must serve each blob");
            assert_eq!(blake3::hash(&got).as_bytes(), hash);
        }
        assert_eq!(weak_pin_count(&peer_b), 3, "each lazy read landed weak-pinned");
        for (_, hash) in &blobs {
            assert!(peer_b.try_local(*hash).is_some(), "resident after the lazy read");
        }

        // The store evicts the first blob: retract the weak pin and
        // drop the bytes. (MemoryRepo has no eviction policy of its
        // own — this is the store-side operation a budgeted store like
        // Yard performs under pressure.)
        {
            let mut store = peer_b.store();
            store
                .unpin_weak(Inline::<Handle<UnknownBlob>>::new(blobs[0].1))
                .unwrap();
            let retained: Vec<Inline<Handle<UnknownBlob>>> = store
                .reader()
                .unwrap()
                .blobs()
                .filter_map(Result::ok)
                .filter(|h| h.raw != blobs[0].1)
                .collect();
            store.keep(retained);
        }

        // The eviction retracted the pin and dropped the resident bytes.
        assert_eq!(weak_pin_count(&peer_b), 2, "weak pin retracted by the eviction");
        assert!(peer_b.try_local(blobs[0].1).is_none(), "oldest evicted from the store");
        assert!(peer_b.try_local(blobs[1].1).is_some(), "second still resident");
        assert!(peer_b.try_local(blobs[2].1).is_some(), "newest still resident");

        // The evicted blob is re-fetchable from the swarm.
        let refetched = drive_future(peer_b.fetch_blob(blobs[0].1), || peer_a.refresh(), 120)
            .await
            .flatten()
            .expect("evicted blob re-fetchable — eviction is always safe");
        assert_eq!(blake3::hash(&refetched).as_bytes(), &blobs[0].1);
    });
}

/// The honest **async** lazy read: `get_or_fetch_async` awaits the
/// swarm fetch (oneshot reply, no blocked thread) and lands the result
/// weak-pinned in the store. Driven deterministically by polling the
/// future and stepping the sim on `Pending` — the awaited oneshot
/// resolves once the host (driven by the stepping) sends the reply.
#[test]
fn async_lazy_read_awaits_swarm_and_lands_weak_pinned() {
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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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

        let got = got
            .expect("want recorded")
            .expect("async lazy read must obtain the blob from the swarm");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "awaited bytes hash to the content id"
        );
        // Landed weak-pinned in the store, served locally on the next read.
        assert!(peer_b.try_local(hash).is_some(), "now resident in the local store");
        assert_eq!(weak_pin_count(&peer_b), 1, "landed under a weak pin");
    });
}

/// Transparent async read through the trait surface: a *generic*
/// `AsyncBlobStoreGet` consumer calls `reader.get(handle).await` on a
/// blob B doesn't hold, and the `PeerReader` fetches it from the swarm
/// and lands it weak-pinned in the shared store — no knowledge that
/// it's a `Peer`. This is the "lazy replication for free" payoff of
/// increment 5b.
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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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
        // The fetch landed in the *shared* store (a &self read mutated
        // Peer state), so a fresh local read now hits.
        assert_eq!(weak_pin_count(&peer_b), 1, "fetch recorded the demand-born weak pin");
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

/// Publisher-first shortcut for read-miss fetches. Gossip already told
/// B who publishes, so the on-demand fetch must consult that knowledge
/// instead of always paying the DHT lookup. Proven under a BLACK-HOLE
/// DHT: previously the on-demand path passed our own id as publisher,
/// so `providers_for`'s publisher-first branch never fired — this fetch
/// resolved Unavailable despite a reachable publisher one gossip hop
/// away. Now the gossip-known publisher serves it directly, no DHT.
#[test]
fn lazy_fetch_uses_gossip_known_publisher_without_dht() {
    use triblespace_core::id::Id;

    let _g = sim_guard();
    run_paused(0x60B1_0001, async {
        let net = SimNet::new(
            0x60B1_0001,
            SimConfig {
                dht: DhtMode::Blackhole,
                ..SimConfig::default()
            },
        );
        let root = key(0xF0);
        let ka = key(0xA0);
        let kb = key(0xB0);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        // A holds a branch (so it gossips a HEAD — that's how B learns
        // A is a publisher) plus an ORPHAN content blob outside the
        // branch closure, so the eager tracking walk cannot land the
        // fetch target at B behind the test's back.
        let (branch_blob, _) = content_blob(0x31);
        let (orphan_blob, orphan_hash) = content_blob(0x32);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(branch_blob.clone()).unwrap();
        store_a.put::<SimpleArchive, _>(orphan_blob.clone()).unwrap();
        store_a
            .update(Id::new([0x77; 16]).unwrap(), None, Some(branch_blob.get_handle()))
            .unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let mut peer_b =
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        // Settle: A's refresh gossips the branch HEAD; B's host notes A
        // as a known publisher when the frame arrives.
        for _ in 0..60u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }
        assert!(
            peer_b.try_local(orphan_hash).is_none(),
            "precondition: the orphan blob never rode the eager walk to B"
        );

        // The DHT is dark, so the ONLY way this fetch can succeed is
        // the gossip-known-publisher shortcut.
        let got = drive_future(peer_b.fetch_blob(orphan_hash), || peer_a.refresh(), 200)
            .await
            .flatten()
            .expect("gossip-known publisher must serve the read-miss fetch without the DHT");
        assert_eq!(blake3::hash(&got).as_bytes(), &orphan_hash);
    });
}

/// The END-TO-END fetch deadline. With a short explicit budget, an
/// unavailable fetch resolves `None` as soon as the budget expires —
/// well before the internal 3s DHT timeout (let alone a stack of
/// per-provider dial/op deadlines) would. Regression test for the
/// previously-unbounded on-demand path, where per-stage deadlines
/// could stack to 40s+ across a provider list.
#[test]
fn fetch_deadline_bounds_unavailable_resolution() {
    let _g = sim_guard();
    run_paused(0xDEAD_0011, async {
        let net = SimNet::new(
            0xDEAD_0011,
            SimConfig {
                dht: DhtMode::Blackhole,
                ..SimConfig::default()
            },
        );
        let root = key(0xFB);
        let ka = key(0xAB);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);

        let store_a = store_with_caps(&[cap_a.clone()]);
        let peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        let _ = net; // keep the sim alive for the fetch

        let (_blob, hash) = content_blob(0x9A);
        // Budget 500 ms; the internal DHT timeout alone is 3 s. 60 sim
        // steps × 20 ms = 1.2 s of virtual time — enough to cross the
        // budget, NOT enough to cross the DHT timeout — so completion
        // within the step allowance proves the overall deadline fired.
        let reply = drive_future(
            peer_a.fetch_blob_with_deadline(hash, Duration::from_millis(500)),
            || {},
            60,
        )
        .await;
        let reply = reply.expect(
            "fetch must resolve within the overall budget, not hang to the DHT timeout",
        );
        assert!(reply.is_none(), "an expired budget is Unavailable, not bytes");
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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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
        assert!(peer_b.try_local(hash).is_none(), "nothing landed from a failed fetch");
        assert_eq!(
            weak_pin_count(&peer_b),
            0,
            "fetch_blob records no want — pinning is the caller's policy"
        );

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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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

/// A default `Peer<S>` **retains** what it fetches: the lazy read lands
/// the blob in the store under a weak pin, so a second read is a LOCAL
/// hit — no re-fetch, no swarm dependency. Proven by crashing the only
/// holder before the second read: it still succeeds, resolving on the
/// first poll without a single sim step. (Under the old two-tier model
/// a cache-less `Peer<S, NullCache>` re-fetched on every read; that
/// behavior no longer exists — retention is the store's job, and every
/// fetch stays resident until the store evicts it.)
#[test]
fn fetched_blob_is_retained_second_read_hits_locally() {
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
        let mut peer_b = bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        let got = drive_future(peer_b.get_or_fetch_async(hash), || peer_a.refresh(), 200)
            .await
            .expect("fetch future completes")
            .expect("want recorded")
            .expect("the lazy read fetches from the swarm");
        assert_eq!(blake3::hash(&got).as_bytes(), &hash);

        // The fetch landed weak-pinned: resident, evictable, retained.
        assert_eq!(weak_pin_count(&peer_b), 1, "fetch landed under a weak pin");
        assert!(peer_b.try_local(hash).is_some(), "a local hit after the fetch");

        // Crash the only holder: the second read must still succeed —
        // it is a local hit, not a re-fetch. `drive_future`'s on_step
        // panicking makes "no sim step needed" an explicit assertion.
        net.crash(pk(&ka));
        let again = drive_future(
            peer_b.get_or_fetch_async(hash),
            || panic!("second read must resolve locally without stepping the sim"),
            1,
        )
        .await
        .expect("second read resolves on the first poll")
        .expect("no want recorded on a local hit")
        .expect("second read is a local hit — no re-fetch");
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
                bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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
/// missing blob. Stresses the shared store (interior-mutable
/// `Arc<Mutex>`): both `&self` reads fetch from the swarm and land into
/// the one shared store. The conn-pool singleflight should share the
/// dial to the holder, and the content-addressed store must end with
/// exactly one copy under exactly one weak pin — no double-store from
/// the racing lands.
#[test]
fn concurrent_transparent_reads_share_store_and_dedupe() {
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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        // Two independent readers off the same Peer — each owns a clone
        // of the store snapshot and a fetch capability into the *same*
        // shared store. (reader() borrows &mut only transiently.)
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
        // Both racing lands hit the same content-addressed store: one
        // copy, and the two recorded wants collapse to one weak pin.
        assert_eq!(
            weak_pin_count(&peer_b),
            1,
            "concurrent lands of the same blob dedupe to a single weak pin"
        );
        assert!(peer_b.try_local(hash).is_some(), "resident after the racing reads");
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
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), true);

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

/// The want-reconcile loop — the daemon half of "a weak pin IS a
/// durable want-marker". A faculty (another process) appends a weak-pin
/// record for a blob the node doesn't hold; the sync daemon's reconcile
/// tick notices the want, fetches the blob from whoever holds it, and
/// lands it under the existing weak pin. Strong pins are never touched,
/// on either side. B runs WITHOUT gossip so the only path the content
/// can take is the reconcile-driven swarm fetch — no eager branch sync
/// can satisfy the want behind the test's back.
#[test]
fn reconcile_tick_services_out_of_band_want() {
    use triblespace_core::id::Id;
    use triblespace_net::reconcile::Reconciler;

    let _g = sim_guard();
    run_paused(0x3A2C, async {
        let net = SimNet::new(0x3A2C, SimConfig::default());
        let root = key(0xFD);
        let ka = key(0xAD);
        let kb = key(0xBD);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);
        let cap_b = admin_cap(&root, &kb);

        // A holds the blob strong-pinned (a branch head points at it) —
        // the eager holder whose retention the reconcile must not touch.
        let (blob, hash) = content_blob(0x21);
        let mut store_a = store_with_caps(&[cap_a.clone(), cap_b.clone()]);
        store_a.put::<SimpleArchive, _>(blob.clone()).unwrap();
        let pin_id = Id::new([0xAD; 16]).unwrap();
        store_a
            .update(pin_id, None, Some(blob.get_handle()))
            .unwrap();
        let store_b = store_with_caps(&[cap_a.clone(), cap_b.clone()]);

        let mut peer_a = bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);
        // B: no gossip — a pure leecher; only the want-reconcile fetches.
        let mut peer_b =
            bring_up(&net, &kb, store_b, team_root, self_cap_of(&cap_b.1), false);

        // Settle the mesh so A's blobs are announced to the DHT.
        for _ in 0..40u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            peer_a.refresh();
        }

        // Out-of-band want: written through the store guard, bypassing
        // the Peer's own read path — exactly what a faculty appending a
        // weak-pin record to the shared pile looks like to the daemon.
        peer_b
            .store()
            .pin_weak(Inline::<Handle<UnknownBlob>>::new(hash))
            .unwrap();
        assert!(peer_b.try_local(hash).is_none(), "precondition: B lacks the blob");

        let a_pins_before: Vec<_> =
            peer_a.store().pins().unwrap().map(Result::unwrap).collect();
        let a_weak_before = weak_pin_count(&peer_a);
        let b_pins_before = peer_b.store().pins().unwrap().count();

        // The reconcile pass: notice the want, fetch, land.
        let mut rec = Reconciler::new();
        let stats = drive_future(rec.tick(&mut peer_b), || peer_a.refresh(), 300)
            .await
            .expect("reconcile tick completes");
        assert_eq!(stats.wants, 1, "the out-of-band weak pin is the want set");
        assert_eq!(stats.missing, 1, "its blob was absent at pass start");
        assert_eq!(stats.fetched, 1, "the want was serviced from the swarm");
        assert_eq!(stats.pending, 0, "nothing left outstanding");

        // The blob landed at B...
        assert!(
            peer_b.try_local(hash).is_some(),
            "want serviced: blob now resident at B"
        );
        // ...still weak-pinned — the want-marker became the retention
        // marker (the reconciler records no pin state of its own)...
        let weak: Vec<_> =
            peer_b.store().weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(
            weak,
            vec![Inline::<Handle<UnknownBlob>>::new(hash)],
            "the want stays on record as the weak pin"
        );
        // ...B grew no strong pin...
        assert_eq!(
            peer_b.store().pins().unwrap().count(),
            b_pins_before,
            "reconcile must not create strong pins"
        );
        // ...and A's retention is untouched.
        let a_pins_after: Vec<_> =
            peer_a.store().pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(a_pins_after, a_pins_before, "A's strong pins untouched");
        assert_eq!(weak_pin_count(&peer_a), a_weak_before, "A's weak pins untouched");
    });
}

/// A want for a handle NOBODY holds stays pending across ticks without
/// erroring — "absent" is always "not obtained yet", never
/// definitely-absent. Also pins down the backoff gate: an immediate
/// re-tick issues no fetch (the failed want waits out its backoff), a
/// re-tick after the backoff elapses retries.
#[test]
fn reconcile_unsatisfiable_want_stays_pending() {
    use triblespace_net::reconcile::Reconciler;

    let _g = sim_guard();
    run_paused(0x9E4D, async {
        // Black-hole DHT: provider lookups time out — the fetch resolves
        // Unavailable in bounded (virtual) time, never hangs.
        let net = SimNet::new(
            0x9E4D,
            SimConfig {
                dht: DhtMode::Blackhole,
                ..SimConfig::default()
            },
        );
        let root = key(0xFE);
        let ka = key(0xAE);
        let team_root = root.verifying_key();
        let cap_a = admin_cap(&root, &ka);

        let store_a = store_with_caps(&[cap_a.clone()]);
        let mut peer_a =
            bring_up(&net, &ka, store_a, team_root, self_cap_of(&cap_a.1), true);

        // A want for content nobody holds (an arbitrary content id).
        let hash = *blake3::hash(b"nobody holds this blob").as_bytes();
        peer_a
            .store()
            .pin_weak(Inline::<Handle<UnknownBlob>>::new(hash))
            .unwrap();

        let mut rec = Reconciler::new();

        // Tick 1: the want is attempted and comes back Unavailable —
        // pending, not an error, not dropped. (No-op on_step: the tick
        // borrows peer_a; stepping alone crosses the DHT deadline.)
        let s1 = drive_future(rec.tick(&mut peer_a), || {}, 400)
            .await
            .expect("tick 1 completes despite the unsatisfiable want");
        assert_eq!(s1.missing, 1);
        assert_eq!(s1.attempted, 1, "first sighting is attempted immediately");
        assert_eq!(s1.fetched, 0);
        assert_eq!(s1.pending, 1, "the want stays pending");

        // Tick 2, immediately: the backoff gate holds — still pending,
        // but no fetch is issued (no hammering a dark swarm).
        let s2 = drive_future(rec.tick(&mut peer_a), || {}, 400)
            .await
            .expect("tick 2 completes");
        assert_eq!(s2.missing, 1);
        assert_eq!(s2.attempted, 0, "backoff-gated: no immediate re-fetch");
        assert_eq!(s2.pending, 1);

        // Let the backoff (1s initial) elapse in virtual time, then
        // tick 3: the want is retried — and stays pending again.
        for _ in 0..100u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
        }
        let s3 = drive_future(rec.tick(&mut peer_a), || {}, 400)
            .await
            .expect("tick 3 completes");
        assert_eq!(s3.attempted, 1, "retried after the backoff elapsed");
        assert_eq!(s3.fetched, 0);
        assert_eq!(s3.pending, 1);

        // Throughout: the want is still durably on record and the blob
        // still absent — nothing was dropped, nothing errored.
        let weak: Vec<_> =
            peer_a.store().weak_pins().unwrap().map(Result::unwrap).collect();
        assert_eq!(weak, vec![Inline::<Handle<UnknownBlob>>::new(hash)]);
        assert!(peer_a.try_local(hash).is_none());
    });
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
