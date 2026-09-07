//! Pool ownership under explicitly polled and cancelled connection requests.

use std::time::Duration;

use futures::poll;

use crate::transport::sim::{SimConfig, SimNet, SimTransport};

use super::*;

fn network() -> (SimNet, Harness<SimTransport>) {
    let net = SimNet::new(
        0xCACE_11ED,
        SimConfig {
            latency: Duration::from_secs(1)..Duration::from_secs(1),
        },
    );
    let client = net.join(&SigningKey::from_bytes(&[255; 32]));
    (net, client)
}

fn join(net: &SimNet, index: usize) -> Harness<SimTransport> {
    let mut seed = [0; 32];
    seed[..8].copy_from_slice(&(index as u64).to_be_bytes());
    net.join(&SigningKey::from_bytes(&seed))
}

#[tokio::test(start_paused = true)]
async fn cancelled_unique_dials_leave_no_uninitialized_pool_entries() {
    let (net, client) = network();
    let pool = new_shared_pool();
    for index in 0..(MAX_CONNECTIONS * 4) {
        let server = join(&net, index);
        let peer = server.transport.local_id();
        net.stall_dials(peer);
        assert!(
            pool_get(&client.transport, &pool, peer)
                .now_or_never()
                .is_none()
        );
        assert_eq!(net.dial_count(client.transport.local_id(), peer), 1);
        tokio::task::yield_now().await;
    }
    let pool = pool.lock().unwrap();
    assert_eq!(
        pool.entries.len(),
        0,
        "cancelled dials must not accumulate outside the bounded connection LRU"
    );
    assert!(pool.least_to_most_recent.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_follower_keeps_the_shared_dial_and_cached_connection() {
    let (net, client) = network();
    let server = join(&net, 1);
    let peer = server.transport.local_id();
    let pool = new_shared_pool();
    let mut first = Box::pin(pool_get(&client.transport, &pool, peer));
    let mut follower = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut follower).is_pending());
    drop(follower);
    assert_eq!(pool.lock().unwrap().entries.len(), 1);

    let mut late = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut late).is_pending());
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 1);
    let (first, late) = tokio::join!(first, late);
    let first = first.unwrap();
    let late = late.unwrap();
    assert!(Arc::ptr_eq(&first.entry, &late.entry));
    let entry = Arc::downgrade(&first.entry);
    drop(first);
    drop(late);

    let cached = pool_get(&client.transport, &pool, peer).await.unwrap();
    assert!(Arc::ptr_eq(&entry.upgrade().unwrap(), &cached.entry));
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 1);
    assert_eq!(pool.lock().unwrap().least_to_most_recent.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_initializer_preserves_waiter_takeover() {
    let (net, client) = network();
    let server = join(&net, 2);
    let peer = server.transport.local_id();
    net.stall_dials(peer);
    let pool = new_shared_pool();
    let mut first = Box::pin(pool_get(&client.transport, &pool, peer));
    let mut follower = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut follower).is_pending());
    let entry = Arc::downgrade(pool.lock().unwrap().entries.get(&peer).unwrap());
    net.unstall_dials(peer);
    drop(first);
    assert!(poll!(&mut follower).is_pending());

    let mut late = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut late).is_pending());
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 2);
    let (follower, late) = tokio::join!(follower, late);
    let follower = follower.unwrap();
    let late = late.unwrap();
    assert!(Arc::ptr_eq(&entry.upgrade().unwrap(), &follower.entry));
    assert!(Arc::ptr_eq(&follower.entry, &late.entry));
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 2);
}

#[tokio::test(start_paused = true)]
async fn cancelling_every_waiter_removes_the_entry_in_either_order() {
    let (net, client) = network();
    let server = join(&net, 3);
    let peer = server.transport.local_id();
    net.stall_dials(peer);
    let pool = new_shared_pool();
    for initializer_first in [true, false] {
        let mut first = Box::pin(pool_get(&client.transport, &pool, peer));
        let mut follower = Box::pin(pool_get(&client.transport, &pool, peer));
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut follower).is_pending());
        let entry = Arc::downgrade(pool.lock().unwrap().entries.get(&peer).unwrap());
        if initializer_first {
            drop(first);
            assert_eq!(pool.lock().unwrap().entries.len(), 1);
            drop(follower);
        } else {
            drop(follower);
            assert_eq!(pool.lock().unwrap().entries.len(), 1);
            drop(first);
        }
        assert!(pool.lock().unwrap().entries.is_empty());
        assert!(entry.upgrade().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_old_waiter_cannot_remove_a_replacement_entry() {
    let (net, client) = network();
    let server = join(&net, 4);
    let peer = server.transport.local_id();
    net.stall_dials(peer);
    let pool = new_shared_pool();
    let mut old = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut old).is_pending());
    let removed = pool.lock().unwrap().entries.remove(&peer).unwrap();
    let old_entry = Arc::downgrade(&removed);
    drop(removed);
    let mut replacement = Box::pin(pool_get(&client.transport, &pool, peer));
    assert!(poll!(&mut replacement).is_pending());
    let new_entry = Arc::downgrade(pool.lock().unwrap().entries.get(&peer).unwrap());
    drop(old);
    assert!(old_entry.upgrade().is_none());
    assert!(Arc::ptr_eq(
        &new_entry.upgrade().unwrap(),
        pool.lock().unwrap().entries.get(&peer).unwrap(),
    ));
    drop(replacement);
    assert!(pool.lock().unwrap().entries.is_empty());
    assert!(new_entry.upgrade().is_none());
}

#[tokio::test(start_paused = true)]
async fn failed_and_timed_out_dials_are_retryable_and_leave_no_entries() {
    let (net, client) = network();
    let server = join(&net, 5);
    let peer = server.transport.local_id();
    let pool = new_shared_pool();
    net.crash(peer);
    assert!(pool_get(&client.transport, &pool, peer).await.is_err());
    assert!(pool.lock().unwrap().entries.is_empty());
    net.revive(peer);
    net.stall_dials(peer);
    assert!(pool_get(&client.transport, &pool, peer).await.is_err());
    assert!(pool.lock().unwrap().entries.is_empty());
    net.unstall_dials(peer);
    assert!(pool_get(&client.transport, &pool, peer).await.is_ok());
    assert_eq!(pool.lock().unwrap().entries.len(), 1);
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 3);
}

#[tokio::test(start_paused = true)]
async fn lru_eviction_preserves_live_borrowers_and_stale_invalidation_is_harmless() {
    let (net, client) = network();
    let mut first_server = join(&net, 0);
    let peer = first_server.transport.local_id();
    let pool = new_shared_pool();
    let first = pool_get(&client.transport, &pool, peer).await.unwrap();
    let first_remote = first_server.incoming.recv().await.unwrap().conn;
    let first_entry = Arc::downgrade(&first.entry);
    for index in 1..=MAX_CONNECTIONS {
        let server = join(&net, index);
        pool_get(&client.transport, &pool, server.transport.local_id())
            .await
            .unwrap();
    }
    {
        let pool = pool.lock().unwrap();
        assert_eq!(pool.entries.len(), MAX_CONNECTIONS);
        assert_eq!(pool.least_to_most_recent.len(), MAX_CONNECTIONS);
        assert!(!pool.entries.contains_key(&peer));
    }
    let (mut send, _recv) = first.conn().open_bi().await.unwrap();
    let (_remote_send, mut remote_recv) = first_remote.accept_bi().await.unwrap();
    send.write_all(b"still live").await.unwrap();
    let mut bytes = [0; 10];
    remote_recv.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still live");

    let replacement = pool_get(&client.transport, &pool, peer).await.unwrap();
    let _replacement_remote = first_server.incoming.recv().await.unwrap().conn;
    assert!(!Arc::ptr_eq(&first.entry, &replacement.entry));
    pool_invalidate(&pool, peer, &first.entry);
    assert!(replacement.conn().open_bi().await.is_ok());
    assert!(first.conn().open_bi().await.is_ok());
    drop(first);
    assert!(first_entry.upgrade().is_none());
    assert_eq!(net.dial_count(client.transport.local_id(), peer), 2);
}
