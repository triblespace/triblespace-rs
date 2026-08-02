//! Focused protocol-ordering regressions over the deterministic transport.
#![cfg(feature = "sim")]

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::clock;
use triblespace_core::id::{ExclusiveId, ufoid};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::macros::entity;
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::BlobStoreGet;
use triblespace_core::repo::capability;
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::Peer;
use triblespace_net::protocol::{OP_GET_BLOB, PILE_SYNC_ALPN, op_auth, send_hash, send_u8};
use triblespace_net::transport::sim::{SimConfig, SimNet};
use triblespace_net::transport::{Conn, Transport};

use common::*;

#[test]
fn authenticated_connection_loses_authority_after_cap_expiry() {
    let _guard = sim_guard();
    run_paused(0xA073, async {
        let net = SimNet::new(0xA073, SimConfig::default());
        let root = key(0xF3);
        let server_key = key(0xA3);
        let client_key = key(0xB3);
        let server_cap = admin_cap(&root, &server_key);
        let client_parent = admin_cap(&root, &client_key);

        let now = clock::epoch_now();
        let expires_at = now + hifitime::Duration::from_seconds(1.0);
        let expiry: Inline<NsTAIInterval> = (now, expires_at).try_to_inline().unwrap();
        let scope_root = *ufoid();
        let scope_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag: capability::PERM_ADMIN,
        });
        let client_cap = capability::build_capability(
            &client_key,
            client_key.verifying_key(),
            client_parent.clone(),
            scope_root,
            scope_facts,
            expiry,
        )
        .expect("build short-lived client capability");

        let server_store =
            store_with_caps(&[server_cap.clone(), client_parent, client_cap.clone()]);
        let _server = bring_up(
            &net,
            &server_key,
            server_store,
            root.verifying_key(),
            self_cap_of(&server_cap.1),
        );

        let client_harness = net.join(pk(&client_key));
        let connection = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial server");
        op_auth(&connection, &self_cap_of(&client_cap.1))
            .await
            .expect("short-lived capability authenticates before expiry");

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("connection remains open before capability expiry");
        // Deliberately split the authorization frame across the expiry
        // boundary. Reading the opcode before expiry must not license the
        // hash/payload that arrives after expiry.
        send_u8(&mut send, OP_GET_BLOB).await.unwrap();
        tokio::task::yield_now().await;
        SimNet::step(&vclock(), Duration::from_secs(2)).await;
        send_hash(&mut send, &server_cap.0.get_handle().raw)
            .await
            .unwrap();
        send.shutdown().await.unwrap();

        let mut response = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), recv.read(&mut response))
            .await
            .expect("expired operation must be rejected promptly")
            .unwrap_or(0);
        assert_eq!(read, 0, "expired authority must receive no blob response");
        assert!(
            connection.open_bi().await.is_err(),
            "the server must close a connection whose verified authority expired"
        );
    });
}

#[test]
fn idle_expired_connection_releases_subject_for_fresh_credential() {
    let _guard = sim_guard();
    run_paused(0xA076, async {
        let net = SimNet::new(0xA076, SimConfig::default());
        let root = key(0xE3);
        let server_key = key(0xC3);
        let client_key = key(0xD3);
        let server_cap = admin_cap(&root, &server_key);
        let client_parent = admin_cap(&root, &client_key);
        let now = clock::epoch_now();

        let build_client_cap = |upper| {
            let scope_root = *ufoid();
            let scope_facts = TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                triblespace_core::metadata::tag: capability::PERM_ADMIN,
            });
            capability::build_capability(
                &client_key,
                client_key.verifying_key(),
                client_parent.clone(),
                scope_root,
                scope_facts,
                (now, upper).try_to_inline().unwrap(),
            )
            .unwrap()
        };
        let expiring = build_client_cap(now + hifitime::Duration::from_seconds(1.0));
        let successor = build_client_cap(now + hifitime::Duration::from_days(1.0));
        let server_store = store_with_caps(&[
            server_cap.clone(),
            client_parent,
            expiring.clone(),
            successor.clone(),
        ]);
        let _server = bring_up(
            &net,
            &server_key,
            server_store,
            root.verifying_key(),
            self_cap_of(&server_cap.1),
        );

        let client_harness = net.join(pk(&client_key));
        let connection = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial server");
        op_auth(&connection, &self_cap_of(&expiring.1))
            .await
            .expect("short-lived credential authenticates");

        // No post-auth stream is sent. The connection-lifetime expiry future
        // itself must clear authority, close transport, and release the TLS
        // subject lease.
        SimNet::step(&vclock(), Duration::from_secs(2)).await;
        assert!(
            connection.open_bi().await.is_err(),
            "idle expired authority must not retain a connection slot"
        );

        let replacement = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial replacement");
        op_auth(&replacement, &self_cap_of(&successor.1))
            .await
            .expect("fresh successor reconnects after idle expiry cleanup");
    });
}

#[test]
fn one_inbound_connection_per_tls_subject_releases_on_close() {
    let _guard = sim_guard();
    run_paused(0xA074, async {
        let net = SimNet::new(0xA074, SimConfig::default());
        let root = key(0xF4);
        let server_key = key(0xA4);
        let client_key = key(0xB4);
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
        );

        let client_harness = net.join(pk(&client_key));
        let first = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial first pile-sync connection");
        op_auth(&first, &client_credential)
            .await
            .expect("first connection authenticates");

        let duplicate = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("transport dial completes before host admission");
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                op_auth(&duplicate, &client_credential)
            )
            .await
            .expect("duplicate subject is rejected promptly")
            .is_err(),
            "the same TLS subject cannot authenticate a second live inbound connection"
        );

        first.close(0, b"replace connection");
        SimNet::step(&vclock(), Duration::from_millis(1)).await;

        let replacement = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial replacement after first closes");
        op_auth(&replacement, &client_credential)
            .await
            .expect("RAII cleanup releases the TLS subject for reconnect");
    });
}

#[test]
fn ninth_executing_stream_for_one_subject_fails_fast() {
    let _guard = sim_guard();
    run_paused(0xA075, async {
        let net = SimNet::new(0xA075, SimConfig::default());
        let root = key(0xF6);
        let server_key = key(0xA6);
        let client_key = key(0xB6);
        let server_cap = admin_cap(&root, &server_key);
        let client_cap = admin_cap(&root, &client_key);
        let server_store = store_with_caps(&[server_cap.clone(), client_cap.clone()]);
        let _server = bring_up(
            &net,
            &server_key,
            server_store,
            root.verifying_key(),
            self_cap_of(&server_cap.1),
        );

        let client_harness = net.join(pk(&client_key));
        let connection = client_harness
            .transport
            .dial(pk(&server_key), PILE_SYNC_ALPN)
            .await
            .expect("dial server");
        op_auth(&connection, &self_cap_of(&client_cap.1))
            .await
            .expect("authenticate connection");

        // Each stream sends only its opcode, leaving the 32-byte hash pending
        // so all eight handlers retain their execution permits.
        let mut stalled = Vec::new();
        for _ in 0..8 {
            let (mut send, recv) = connection.open_bi().await.expect("open admitted stream");
            send_u8(&mut send, OP_GET_BLOB).await.unwrap();
            stalled.push((send, recv));
        }
        tokio::task::yield_now().await;

        let (mut ninth_send, mut ninth_recv) = connection
            .open_bi()
            .await
            .expect("ninth stream reaches fail-fast host admission");
        send_u8(&mut ninth_send, OP_GET_BLOB).await.unwrap();
        let mut response = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), ninth_recv.read(&mut response))
            .await
            .expect("ninth stream is rejected promptly")
            .unwrap_or(0);
        assert_eq!(read, 0, "the ninth stream must not become a waiter");
        assert!(
            connection.open_bi().await.is_err(),
            "per-subject saturation closes the offending connection"
        );
        drop(stalled);
    });
}

#[test]
fn recipient_retains_delivered_delegation_and_missing_parent_chain() {
    let _guard = sim_guard();
    run_paused(0xDE11_7E01, async {
        let net = SimNet::new(0xDE11_7E01, SimConfig::default());
        let root = key(0xF5);
        let issuer_key = key(0xA5);
        let recipient_key = key(0xB5);
        let team_root = root.verifying_key();
        let issuer_cap = admin_cap(&root, &issuer_key);
        let recipient_old_cap = admin_cap(&root, &recipient_key);

        let now = clock::epoch_now();
        let expiry: Inline<NsTAIInterval> = (now, now + hifitime::Duration::from_days(30.0))
            .try_to_inline()
            .unwrap();
        let (delivered_cap, delivered_sig) = capability::build_capability(
            &issuer_key,
            recipient_key.verifying_key(),
            issuer_cap.clone(),
            *ufoid(),
            TribleSet::new(),
            expiry,
        )
        .expect("build delegated capability");

        // The issuer can verify and serve the complete new chain. The
        // recipient deliberately starts with only its unrelated old direct
        // cap; the delivered leaf arrives in-band and its parent must be
        // fetched exactly from the TLS-authenticated issuer.
        let issuer_store = store_with_caps(&[
            issuer_cap.clone(),
            (delivered_cap.clone(), delivered_sig.clone()),
        ]);
        let mut recipient_store = store_with_caps(&[recipient_old_cap.clone()]);
        triblespace_net::policy::record_outbound_cap_request(
            &mut recipient_store,
            delivered_cap.clone(),
        )
        .expect("record local delivery intent");

        let issuer_harness = net.join(pk(&issuer_key));
        let issuer_id = iroh_base::EndpointId::from_bytes(&pk(&issuer_key)).unwrap();
        let (issuer_sender, issuer_receiver, issuer_wiring) = host::wire(issuer_id);
        tokio::task::spawn_local(host::run_host(
            issuer_harness,
            triblespace_net::peer::PeerConfig {
                peers: Vec::new(),
                team_root,
                self_cap: self_cap_of(&issuer_cap.1),
            },
            issuer_wiring,
        ));
        let deliver = issuer_sender.clone();
        let mut issuer = Peer::with_wiring(
            issuer_store,
            issuer_key.clone(),
            team_root,
            issuer_sender,
            issuer_receiver,
        );
        let mut recipient = bring_up(
            &net,
            &recipient_key,
            recipient_store,
            team_root,
            self_cap_of(&recipient_old_cap.1),
        );

        deliver.deliver_cap(
            pk(&recipient_key),
            delivered_cap.bytes.clone(),
            delivered_sig.bytes.clone(),
        );
        for _ in 0..120u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            issuer.refresh().unwrap();
            recipient.refresh().unwrap();
        }

        let reader = recipient.reader().unwrap();
        let delivered_sig_handle: Inline<Handle<SimpleArchive>> = delivered_sig.get_handle();
        let parent_cap_handle: Inline<Handle<SimpleArchive>> = issuer_cap.0.get_handle();
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&reader, delivered_sig_handle).is_ok(),
            "the verified in-band leaf signature must be absorbed"
        );
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&reader, parent_cap_handle).is_ok(),
            "the exact missing parent capability must be fetched and cached"
        );
    });
}

#[test]
fn successful_delivery_rotates_outbound_auth_and_evicts_predecessor_pool() {
    let _guard = sim_guard();
    run_paused(0xC0DE_A071, async {
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::blob::encodings::longstring::LongString;
        use triblespace_core::repo::BlobStorePut;

        let net = SimNet::new(0xC0DE_A071, SimConfig::default());
        let root = key(0xD1);
        let client_key = key(0xD2);
        let server_key = key(0xD3);
        let team_root = root.verifying_key();
        let now = clock::epoch_now();

        let anchor_scope = *ufoid();
        let anchor_facts = TribleSet::from(entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag: capability::PERM_ADMIN,
        });
        let anchor = capability::build_founder_anchor(
            &root,
            server_key.verifying_key(),
            anchor_scope,
            anchor_facts,
        )
        .unwrap();

        let build_server_cap = |subject| {
            let scope_root = *ufoid();
            let scope_facts = TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                triblespace_core::metadata::tag: capability::PERM_ADMIN,
            });
            capability::build_capability(
                &server_key,
                subject,
                anchor.clone(),
                scope_root,
                scope_facts,
                (now, now + hifitime::Duration::from_days(1.0))
                    .try_to_inline()
                    .unwrap(),
            )
            .unwrap()
        };
        let server_cap = build_server_cap(server_key.verifying_key());

        let build_client_cap = |upper: hifitime::Epoch| {
            let scope_root = *ufoid();
            let scope_facts = TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                triblespace_core::metadata::tag: capability::PERM_ADMIN,
            });
            capability::build_capability(
                &server_key,
                client_key.verifying_key(),
                anchor.clone(),
                scope_root,
                scope_facts,
                (now, upper).try_to_inline().unwrap(),
            )
            .unwrap()
        };
        let predecessor = build_client_cap(now + hifitime::Duration::from_seconds(30.0));
        let successor = build_client_cap(now + hifitime::Duration::from_days(1.0));

        let mut server_store = store_with_caps(&[
            anchor.clone(),
            server_cap.clone(),
            predecessor.clone(),
            successor.clone(),
        ]);
        let content: Inline<Handle<LongString>> = server_store
            .put("credential rotation probe".to_owned().to_blob())
            .unwrap();
        let server_harness = net.join(pk(&server_key));
        let server_id = iroh_base::EndpointId::from_bytes(&pk(&server_key)).unwrap();
        let (server_sender, server_receiver, server_wiring) = host::wire(server_id);
        tokio::task::spawn_local(host::run_host(
            server_harness,
            triblespace_net::peer::PeerConfig {
                peers: Vec::new(),
                team_root,
                self_cap: self_cap_of(&server_cap.1),
            },
            server_wiring,
        ));
        let deliver = server_sender.clone();
        let mut server = Peer::with_wiring(
            server_store,
            server_key.clone(),
            team_root,
            server_sender,
            server_receiver,
        );

        let mut client_store = store_with_caps(std::slice::from_ref(&predecessor));
        triblespace_net::policy::record_outbound_cap_request(
            &mut client_store,
            successor.0.clone(),
        )
        .expect("record successor expectation");
        let mut client = bring_up(
            &net,
            &client_key,
            client_store,
            team_root,
            self_cap_of(&predecessor.1),
        );

        for _ in 0..20 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            server.refresh().unwrap();
            client.refresh().unwrap();
        }
        assert_eq!(
            client.fetch_blob(content.raw).await.as_deref(),
            Some("credential rotation probe".as_bytes()),
            "the predecessor credential establishes the initial pooled connection"
        );

        deliver.deliver_cap(
            pk(&client_key),
            successor.0.bytes.clone(),
            successor.1.bytes.clone(),
        );
        for _ in 0..100 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            server.refresh().unwrap();
            client.refresh().unwrap();
        }
        let successor_handles = (successor.0.get_handle(), successor.1.get_handle());
        {
            let mut store = client.store();
            assert_eq!(
                triblespace_net::policy::current_team_cap(&mut *store, team_root),
                Some(successor_handles),
                "the delivered successor is durably active before host rotation"
            );
        }

        SimNet::step(&vclock(), Duration::from_secs(31)).await;
        assert_eq!(
            client.fetch_blob(content.raw).await.as_deref(),
            Some("credential rotation probe".as_bytes()),
            "after predecessor expiry, a fresh dial must authenticate with the successor"
        );
    });
}

#[test]
fn renewal_tick_delivers_freshly_persisted_cap_to_cold_recipient() {
    let _guard = sim_guard();
    run_paused(0xA11C_E001, async {
        let net = SimNet::new(0xA11C_E001, SimConfig::default());
        let root = key(0xF6);
        let issuer_key = key(0xA6);
        let recipient_key = key(0xB6);
        let team_root = root.verifying_key();
        let issuer_cap = admin_cap(&root, &issuer_key);
        let recipient_self_cap = admin_cap(&root, &recipient_key);

        let now = clock::epoch_now();
        let renewal_window = hifitime::Duration::from_hours(1.0);
        let old_expiry: Inline<NsTAIInterval> =
            (now, now + hifitime::Duration::from_seconds(300.0))
                .try_to_inline()
                .unwrap();
        let requested_expiry: Inline<NsTAIInterval> =
            (now, now + hifitime::Duration::from_days(1.0))
                .try_to_inline()
                .unwrap();
        let scope_root = *ufoid();

        let (old_cap, old_sig) = capability::build_capability(
            &issuer_key,
            recipient_key.verifying_key(),
            issuer_cap.clone(),
            scope_root,
            TribleSet::new(),
            old_expiry,
        )
        .expect("build expiring delegation");
        let old_cap_handle: Inline<Handle<SimpleArchive>> = old_cap.get_handle();
        let old_sig_handle: Inline<Handle<SimpleArchive>> = old_sig.get_handle();

        // The request records a deliberately wider expiry ceiling than the
        // successor renewal_tick will mint. It is local selection intent, not
        // the delivered credential, and its signature is never installed.
        let (requested_cap, _requested_sig) = capability::build_capability(
            &issuer_key,
            recipient_key.verifying_key(),
            issuer_cap.clone(),
            scope_root,
            TribleSet::new(),
            requested_expiry,
        )
        .expect("build local request intent");

        // Only the *old* delegation exists before the tick. The fresh pair is
        // created and persisted inside renewal_tick, so the serving snapshot
        // must be rebuilt before the resulting OP_DELIVER_CAP is dispatched.
        let mut issuer_store =
            store_with_caps(&[issuer_cap.clone(), (old_cap.clone(), old_sig.clone())]);
        triblespace_net::policy::pin_team_cap(
            &mut issuer_store,
            team_root,
            issuer_cap.0.get_handle(),
            issuer_cap.1.get_handle(),
        )
        .expect("pin issuer team capability");
        let policy_entry = triblespace_net::policy::record_policy_entry(
            &mut issuer_store,
            recipient_key.verifying_key(),
            scope_root,
            old_expiry,
            old_cap_handle,
            old_sig_handle,
        )
        .expect("record expiring renewal entry");
        triblespace_net::policy::mark_policy_delivered(&mut issuer_store, policy_entry)
            .expect("suppress redispatch of the old delegation");

        // The recipient is cold with respect to the issuer: it has neither
        // the issuer's root delegation nor either version of the recipient
        // delegation. The narrow delivery proof path must fetch the parent.
        let mut recipient_store = store_with_caps(&[recipient_self_cap.clone()]);
        triblespace_net::policy::record_outbound_cap_request(&mut recipient_store, requested_cap)
            .expect("record local first-delivery intent");
        let cold_reader = recipient_store.reader().unwrap();
        let issuer_cap_handle: Inline<Handle<SimpleArchive>> = issuer_cap.0.get_handle();
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&cold_reader, issuer_cap_handle).is_err(),
            "recipient must begin without the issuer's parent capability"
        );
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&cold_reader, old_sig_handle).is_err(),
            "recipient must begin without the old delivered credential"
        );
        drop(cold_reader);

        let mut issuer = bring_up(
            &net,
            &issuer_key,
            issuer_store,
            team_root,
            self_cap_of(&issuer_cap.1),
        );
        let mut recipient = bring_up(
            &net,
            &recipient_key,
            recipient_store,
            team_root,
            self_cap_of(&recipient_self_cap.1),
        );

        assert_eq!(
            issuer.renewal_tick(renewal_window),
            1,
            "the due entry should mint and dispatch exactly one fresh successor"
        );
        let (fresh_cap_handle, fresh_sig_handle) = {
            let mut store = issuer.store();
            let mut entries = triblespace_net::policy::list_renewal_policy(&mut *store);
            assert_eq!(entries.len(), 1);
            let entry = entries.pop().unwrap();
            (entry.latest_cap, entry.latest_sig)
        };
        assert_ne!(fresh_cap_handle, old_cap_handle);
        assert_ne!(fresh_sig_handle, old_sig_handle);

        for _ in 0..120u32 {
            SimNet::step(&vclock(), Duration::from_millis(20)).await;
            issuer.refresh().unwrap();
            recipient.refresh().unwrap();
        }

        let reader = recipient.reader().unwrap();
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&reader, fresh_cap_handle).is_ok(),
            "recipient must persist the just-minted capability"
        );
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&reader, fresh_sig_handle).is_ok(),
            "recipient must persist the just-minted signature"
        );
        assert!(
            BlobStoreGet::get::<TribleSet, SimpleArchive>(&reader, issuer_cap_handle).is_ok(),
            "cold recipient must fetch and retain the exact missing parent proof"
        );
        drop(reader);

        let mut store = recipient.store();
        assert_eq!(
            triblespace_net::policy::current_team_cap(&mut *store, team_root),
            Some((fresh_cap_handle, fresh_sig_handle)),
            "the fresh pair must become the active team credential"
        );
        assert!(
            triblespace_net::policy::expected_outbound_cap_request(&mut *store).is_none(),
            "successful first delivery must consume the local expectation"
        );
    });
}

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
        );

        let client_harness = net.join(pk(&client_key));
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
        );

        let client_harness = net.join(pk(&client_key));
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
