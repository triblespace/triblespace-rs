//! Focused protocol-ordering regressions over the deterministic transport.
#![cfg(feature = "sim")]

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::prelude::BlobStorePut;
use triblespace_net::protocol::{
    AUTH_OK, NIL_HASH, OP_AUTH, OP_CHILDREN, OP_GET_BLOB, PILE_SYNC_ALPN, op_auth, recv_hash,
    recv_u8, send_hash, send_u8, send_u64_be,
};
use triblespace_net::transport::sim::{SimConfig, SimNet};
use triblespace_net::transport::{Conn, GossipSink, Transport};

use common::*;

#[test]
fn non_auth_first_stream_closes_the_connection() {
    let _guard = sim_guard();
    run_paused(0xA071, async {
        let net = SimNet::new(0xA071, SimConfig::default());
        let root = key(0xF0);
        let server_key = key(0xA0);
        let client_key = key(0xB0);
        let server_cap = admin_cap(&root, &server_key);
        let client_cap = admin_cap(&root, &client_key);
        let server_store = store_with_caps(&[server_cap.clone(), client_cap]);
        let _server = bring_up(
            &net,
            &server_key,
            server_store,
            root.verifying_key(),
            self_cap_of(&server_cap.1),
            false,
        );

        let client_harness = net.join(pk(&client_key), false);
        let connection = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial server");
        let (mut send, mut recv) = connection.open_bi().await.expect("open first stream");
        send_u8(&mut send, OP_GET_BLOB).await.unwrap();
        send_hash(&mut send, &[9; 32]).await.unwrap();
        send.shutdown().await.unwrap();

        let mut response = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), recv.read(&mut response))
            .await
            .expect("server must close a non-auth first stream")
            .unwrap_or(0);
        assert_eq!(read, 0, "unauthenticated request must receive no data");
        assert!(
            connection.open_bi().await.is_err(),
            "rejecting the mandatory first stream must close the connection"
        );
    });
}

#[test]
fn op_auth_is_not_a_per_stream_reauthentication_operation() {
    let _guard = sim_guard();
    run_paused(0xA072, async {
        let net = SimNet::new(0xA072, SimConfig::default());
        let root = key(0xF1);
        let server_key = key(0xA1);
        let client_key = key(0xB1);
        let server_cap = admin_cap(&root, &server_key);
        let client_cap = admin_cap(&root, &client_key);
        let client_credential = self_cap_of(&client_cap.1);
        let server_store = store_with_caps(&[server_cap.clone(), client_cap]);
        let _server = bring_up(
            &net,
            &server_key,
            server_store,
            root.verifying_key(),
            self_cap_of(&server_cap.1),
            false,
        );

        let client_harness = net.join(pk(&client_key), false);
        let connection = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial server");
        op_auth(&connection, &client_credential)
            .await
            .expect("first-stream auth succeeds");
        assert!(
            op_auth(&connection, &client_credential).await.is_err(),
            "a later OP_AUTH stream must be rejected instead of replacing connection state"
        );
    });
}

/// A claimed publisher may return correct parent bytes while lying about both
/// child enumeration and child content. The client must bind alternate hints
/// to the verified parent, reject the publisher's bogus child bytes, and fall
/// through to an honest DHT holder.
#[test]
fn hinted_subgraph_falls_back_from_bogus_publisher() {
    let _guard = sim_guard();
    run_paused(0xC105_ED01, async {
        let net = SimNet::new(0xC105_ED01, SimConfig::default());
        let root_key = key(0xF2);
        let malicious_key = key(0xA2);
        let client_key = key(0xB2);
        let holder_key = key(0xC2);
        let team_root = root_key.verifying_key();
        let cap_a = admin_cap(&root_key, &malicious_key);
        let cap_b = admin_cap(&root_key, &client_key);
        let cap_c = admin_cap(&root_key, &holder_key);
        let all = [cap_a.clone(), cap_b.clone(), cap_c.clone()];

        let (child, child_hash) = {
            let bytes = anybytes::Bytes::from_source(vec![0x42; 17]);
            let blob = Blob::<UnknownBlob>::new(bytes);
            let hash = blob.get_handle().raw;
            (blob, hash)
        };
        // The parent intrinsically contains the child's content handle as its
        // sole aligned word.
        let parent_bytes = child_hash.to_vec();
        let parent = Blob::<UnknownBlob>::new(anybytes::Bytes::from_source(parent_bytes.clone()));
        let parent_hash = parent.get_handle().raw;

        let mut holder_store = store_with_caps(&all);
        holder_store.put::<UnknownBlob, _>(child).unwrap();
        holder_store.put::<UnknownBlob, _>(parent).unwrap();
        let client_store = store_with_caps(&all);
        let mut holder = bring_up(
            &net,
            &holder_key,
            holder_store,
            team_root,
            self_cap_of(&cap_c.1),
            false,
        );
        let mut client = bring_up(
            &net,
            &client_key,
            client_store,
            team_root,
            self_cap_of(&cap_b.1),
            true,
        );

        // A custom authenticated-by-transport peer: accepts AUTH, serves the
        // requested parent correctly, lies that it has no children, and serves
        // wrong bytes for every child request.
        let mut malicious = net.join(pk(&malicious_key), true);
        let (gossip, _gossip_events) = malicious.gossip.take().expect("gossip joined");
        let mut incoming = malicious.incoming;
        tokio::task::spawn_local(async move {
            while let Some(incoming) = incoming.recv().await {
                if incoming.alpn != PILE_SYNC_ALPN {
                    continue;
                }
                let connection = incoming.conn;
                let parent_bytes = parent_bytes.clone();
                tokio::task::spawn_local(async move {
                    let Some((mut auth_send, mut auth_recv)) = connection.accept_bi().await else {
                        return;
                    };
                    if recv_u8(&mut auth_recv).await.ok() != Some(OP_AUTH) {
                        connection.close(0, b"auth required");
                        return;
                    }
                    let _ = recv_hash(&mut auth_recv).await;
                    let _ = send_u8(&mut auth_send, AUTH_OK).await;
                    let _ = auth_send.shutdown().await;

                    while let Some((mut send, mut recv)) = connection.accept_bi().await {
                        match recv_u8(&mut recv).await {
                            Ok(OP_GET_BLOB) => {
                                let Ok(requested) = recv_hash(&mut recv).await else {
                                    break;
                                };
                                let data = if requested == parent_hash {
                                    parent_bytes.clone()
                                } else {
                                    vec![0xEE; 19]
                                };
                                let _ = send_u64_be(&mut send, data.len() as u64).await;
                                let _ = send.write_all(&data).await;
                                let _ = send.shutdown().await;
                            }
                            Ok(OP_CHILDREN) => {
                                let _ = recv_hash(&mut recv).await;
                                // False leaf: the honest holder's DHT response
                                // must still be consulted.
                                let _ = send_hash(&mut send, &NIL_HASH).await;
                                let _ = send.shutdown().await;
                            }
                            _ => break,
                        }
                    }
                });
            }
        });

        // Announce the honest holder's parent and child to the DHT before the
        // malicious publisher's gossip observation starts the walk.
        for _ in 0..30u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            holder.refresh().unwrap();
        }
        let mut frame = Vec::with_capacity(89);
        frame.push(0x02);
        frame.extend_from_slice(&[0x51; 16]);
        frame.extend_from_slice(&parent_hash);
        frame.extend_from_slice(&pk(&malicious_key));
        frame.extend_from_slice(&1u64.to_be_bytes());
        gossip.broadcast(frame).await.unwrap();

        for _ in 0..300u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            holder.refresh().unwrap();
            client.refresh().unwrap();
            if client.try_local(parent_hash).is_some() && client.try_local(child_hash).is_some() {
                break;
            }
        }
        assert!(
            client.try_local(parent_hash).is_some(),
            "verified parent bytes must land"
        );
        assert!(
            client.try_local(child_hash).is_some(),
            "bogus publisher child bytes must fall through to the honest holder"
        );
    });
}
