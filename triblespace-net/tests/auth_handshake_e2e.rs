//! End-to-end test of the auth-handshake ALPN: two iroh endpoints
//! sharing a `TestNetwork` exchange `OP_REQUEST_CAP` and
//! `OP_DELIVER_CAP` over a real iroh transport (no relays, no DNS).
//!
//! This catches wire-format bugs that pure unit tests would miss,
//! and validates that the protocol can be driven from arbitrary code
//! (the public surface in `triblespace_net::handshake` is
//! sufficient — no leakage of private types required).

use ed25519_dalek::SigningKey;
use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::test_utils::test_transport::{TestNetwork, to_custom_addr};
use iroh_base::{EndpointAddr, EndpointId, SecretKey, TransportAddr};
use rand::rngs::OsRng;
use tokio::sync::mpsc;

use triblespace_net::handshake::{
    AUTH_HANDSHAKE_ALPN, IncomingOp, STATUS_OK, read_incoming, respond, send_deliver_cap,
    send_request_cap,
};

/// Minimal protocol handler: parses incoming streams and forwards
/// decoded ops to a channel. Sufficient for protocol-level tests —
/// production code also runs policy on top of these events, but the
/// wire decode is the part we want to validate here.
#[derive(Clone)]
struct EventForwardingHandler {
    events: mpsc::UnboundedSender<IncomingOp>,
}

impl std::fmt::Debug for EventForwardingHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventForwardingHandler").finish()
    }
}

impl iroh::protocol::ProtocolHandler for EventForwardingHandler {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let events = self.events.clone();
        // Loop on accept_bi until the remote closes — same pattern as
        // the production `HandshakeHandler`. Returning before the
        // remote has cleanly torn down the connection races with the
        // response-flush and surfaces as "connection lost" on the
        // client's recv_status.
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            if let Ok(Some(op)) = read_incoming(&mut recv).await {
                let _ = events.send(op);
                let _ = respond(&mut send, STATUS_OK).await;
            }
        }
        Ok(())
    }
}

/// Construct an iroh endpoint over the supplied test transport.
/// Mirrors what `triblespace_net::handshake::one_shot_endpoint` does
/// for production (dot-stripped relay + mDNS + DHT lookups), but
/// strips all of those — the TestNetwork virtual transport is the
/// only path that matters for the test.
async fn test_endpoint(
    network: &TestNetwork,
    secret: SecretKey,
) -> Endpoint {
    let transport = network
        .create_transport(secret.public())
        .expect("create test transport");
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .relay_mode(iroh::RelayMode::Disabled)
        .ca_roots_config(iroh::tls::CaRootsConfig::insecure_skip_verify())
        .add_custom_transport(transport);
    builder = builder.clear_ip_transports();
    builder.bind().await.expect("bind endpoint")
}

fn custom_addr(id: EndpointId) -> EndpointAddr {
    EndpointAddr::from_parts(
        id,
        std::iter::once(TransportAddr::Custom(to_custom_addr(id))),
    )
}

fn to_iroh_secret(key: &SigningKey) -> SecretKey {
    SecretKey::from_bytes(&key.to_bytes())
}

#[tokio::test]
async fn op_request_cap_round_trips() {
    let network = TestNetwork::new();

    let admin_key = SigningKey::generate(&mut OsRng);
    let requester_key = SigningKey::generate(&mut OsRng);

    let admin_secret = to_iroh_secret(&admin_key);
    let requester_secret = to_iroh_secret(&requester_key);
    let admin_id = EndpointId::from(admin_secret.public());

    let admin_ep = test_endpoint(&network, admin_secret).await;
    let requester_ep = test_endpoint(&network, requester_secret).await;

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<IncomingOp>();
    let handler = EventForwardingHandler { events: events_tx };
    let _router = Router::builder(admin_ep.clone())
        .accept(AUTH_HANDSHAKE_ALPN, handler)
        .spawn();

    // Requester dials admin and sends a partial-cap blob.
    let payload: Vec<u8> = b"hello-partial-cap-bytes".to_vec();
    let conn = requester_ep
        .connect(custom_addr(admin_id), AUTH_HANDSHAKE_ALPN)
        .await
        .expect("connect");
    let status = send_request_cap(&conn, &payload).await.expect("send");
    assert_eq!(status, STATUS_OK, "admin should ACK the request");
    conn.close(0u32.into(), b"ok");

    // Admin's handler should have decoded the op + forwarded it.
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), events_rx.recv())
        .await
        .expect("event arrives within 2s")
        .expect("channel still open");
    match event {
        IncomingOp::Request { partial_cap_bytes } => {
            assert_eq!(partial_cap_bytes, payload, "payload survives the wire");
        }
        IncomingOp::Deliver { .. } => panic!("got Deliver, expected Request"),
    }
}

#[tokio::test]
async fn op_deliver_cap_round_trips() {
    let network = TestNetwork::new();

    let issuer_key = SigningKey::generate(&mut OsRng);
    let recipient_key = SigningKey::generate(&mut OsRng);

    let issuer_secret = to_iroh_secret(&issuer_key);
    let recipient_secret = to_iroh_secret(&recipient_key);
    let recipient_id = EndpointId::from(recipient_secret.public());

    let issuer_ep = test_endpoint(&network, issuer_secret).await;
    let recipient_ep = test_endpoint(&network, recipient_secret).await;

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<IncomingOp>();
    let handler = EventForwardingHandler { events: events_tx };
    let _router = Router::builder(recipient_ep.clone())
        .accept(AUTH_HANDSHAKE_ALPN, handler)
        .spawn();

    let cap_bytes: Vec<u8> = b"cap-blob-bytes-here".to_vec();
    let sig_bytes: Vec<u8> = b"sig-blob-bytes-here".to_vec();
    let conn = issuer_ep
        .connect(custom_addr(recipient_id), AUTH_HANDSHAKE_ALPN)
        .await
        .expect("connect");
    let status = send_deliver_cap(&conn, &cap_bytes, &sig_bytes)
        .await
        .expect("send");
    assert_eq!(status, STATUS_OK, "recipient should ACK the delivery");
    conn.close(0u32.into(), b"ok");

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), events_rx.recv())
        .await
        .expect("event arrives within 2s")
        .expect("channel still open");
    match event {
        IncomingOp::Deliver { cap_bytes: c, sig_bytes: s } => {
            assert_eq!(c, cap_bytes, "cap payload survives the wire");
            assert_eq!(s, sig_bytes, "sig payload survives the wire");
        }
        IncomingOp::Request { .. } => panic!("got Request, expected Deliver"),
    }
}
