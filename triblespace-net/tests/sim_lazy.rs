//! Lazy-replication read path — deterministic simulation.
//!
//! Exercises the swarm-addressed on-demand fetch
//! (`Peer::request_blob` / `NetCommand::FetchBlob`) and, as the build
//! progresses, the `PeerReader` fall-through and pin policies. The
//! property under test: a node which does NOT hold a content blob can
//! still obtain it from whoever in the swarm does — without every node
//! eagerly replicating everything.
//!
//! Sim note: `fetch_blob` (the blocking wrapper) cannot be tested on
//! the single-threaded paused-time runtime — blocking the one thread
//! would freeze the host that must produce the reply. So these drive
//! the non-blocking `request_blob` and step the sim while polling the
//! reply (`try_recv`), which is the deterministic-sim idiom.
#![cfg(feature = "sim")]

mod common;

use std::time::Duration;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::Blob;
use triblespace_core::blob::IntoBlob;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::BlobStore;
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
    rx: &std::sync::mpsc::Receiver<Option<Vec<u8>>>,
    mut on_step: F,
    steps: u32,
) -> Option<Vec<u8>> {
    for _ in 0..steps {
        SimNet::step(&vclock(), Duration::from_millis(20)).await;
        on_step();
        match rx.try_recv() {
            Ok(reply) => return reply,
            Err(std::sync::mpsc::TryRecvError::Empty) => continue,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
        }
    }
    None
}

fn holds_locally(peer: &mut triblespace_net::peer::Peer<triblespace_core::repo::memoryrepo::MemoryRepo>, hash: [u8; 32]) -> bool {
    peer.reader()
        .unwrap()
        .get::<anybytes::Bytes, UnknownBlob>(Inline::new(hash))
        .is_ok()
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

        let rx = peer_b.request_blob(hash);
        let got = drive_until(&rx, || peer_a.refresh(), 120)
            .await
            .expect("B must obtain the blob from the swarm");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "fetched bytes must hash to the requested content id"
        );
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
        let rx = peer_a.request_blob(hash);
        // providers_for has a 3s internal DHT timeout; give the sim
        // enough virtual steps to cross it, then expect a None reply.
        let reply = drive_until(&rx, || peer_a.refresh(), 400).await;
        assert!(
            reply.is_none(),
            "unavailable fetch must resolve to None, got {:?} bytes",
            reply.map(|b| b.len())
        );
    });
}
