//! Asymmetric capability handshake protocol.
//!
//! This ALPN is the entry point for un-onboarded peers (peers who don't
//! yet hold a cap chaining to a team root) and the delivery channel for
//! renewals. It runs alongside `PILE_SYNC_ALPN`, not in place of it.
//!
//! Two one-shot operations, no shared state held across human-approval
//! latency:
//!
//!   `OP_REQUEST_CAP` (subject → issuer)
//!     subject sends a partial cap blob (subject + scope + expiry it
//!     wants). issuer responds with an ACK byte and closes. issuer then
//!     either: (a) auto-approves via its renewal-policy branch and
//!     dispatches `OP_DELIVER_CAP` in the daemon's next tick, or
//!     (b) queues the request for human approval — when the human
//!     approves, the daemon dispatches `OP_DELIVER_CAP`.
//!
//!   `OP_DELIVER_CAP` (issuer → subject)
//!     issuer ships the signed (cap, sig) bytes. subject ACKs and
//!     closes. subject verifies + pins the cap into its team-cap
//!     branch.
//!
//! Connection-level pubkey auth (iroh's QUIC TLS) is enough: A knows
//! K_B from `connection.remote_id()`, B knows K_A from the dial
//! target. No protocol-layer signature on the request is needed for v1.
//!
//! See `decide#4b6edde7` for the architectural decision and
//! `decide#4b321c47` for the surrounding cap-system design.

use anyhow::{Result, anyhow};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::transport::Conn;

/// ALPN for the auth handshake. Distinct from `PILE_SYNC_ALPN` —
/// connections here are open to any pubkey-bearing peer; identity is
/// inferred from iroh's TLS layer, not via a separate `OP_AUTH` step.
pub const AUTH_HANDSHAKE_ALPN: &[u8] = b"/triblespace/auth-handshake/1";

/// Subject → issuer. Body: u32 length + partial cap blob bytes
/// (subject prepares it, issuer fills in chain linkage at sign time).
/// Response: 1-byte status.
pub const OP_REQUEST_CAP: u8 = 0x01;

/// Issuer → subject. Body: u32 cap-blob len + cap bytes + u32 sig-blob
/// len + sig bytes. Response: 1-byte status.
pub const OP_DELIVER_CAP: u8 = 0x02;

/// Status: request received and queued (sender should expect a later
/// `OP_DELIVER_CAP` push from the issuer, or no response if denied).
pub const STATUS_OK: u8 = 0x00;
/// Status: request rejected (e.g. issuer is not an admin of the
/// requested team; sender's pubkey is not eligible). Sender shouldn't
/// retry until something changes out-of-band.
pub const STATUS_REJECTED: u8 = 0x01;
/// Status: payload malformed (couldn't decode the partial cap, length
/// prefix exceeds bounds, etc.). Bug or version mismatch.
pub const STATUS_MALFORMED: u8 = 0x02;

/// Hard cap on per-blob payload size accepted on the wire — defensive
/// against memory abuse from misbehaving peers. Real cap blobs are
/// well under this; sig blobs grow linearly in chain depth (~100B per
/// level) but a 16 KiB sig blob would mean ~160 chain levels, way past
/// any plausible deployment.
pub const MAX_BLOB_BYTES: u32 = 16 * 1024;

// ── Client-side: send operations on a fresh stream ────────────────────

/// Send `OP_REQUEST_CAP` on a fresh stream of `conn`. Returns the
/// issuer's status byte.
///
/// `partial_cap_bytes` is a SimpleArchive-encoded cap blob the subject
/// has prepared (declaring subject pubkey, scope root, expiry, and
/// scope facts). The issuer will fill in chain linkage and re-sign.
pub async fn send_request_cap<C: Conn>(conn: &C, partial_cap_bytes: &[u8]) -> Result<u8> {
    if partial_cap_bytes.len() > MAX_BLOB_BYTES as usize {
        return Err(anyhow!(
            "partial cap is {} bytes, exceeds MAX_BLOB_BYTES {}",
            partial_cap_bytes.len(),
            MAX_BLOB_BYTES
        ));
    }
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send.write_all(&[OP_REQUEST_CAP])
        .await
        .map_err(|e| anyhow!("send op: {e}"))?;
    send.write_all(&(partial_cap_bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|e| anyhow!("send len: {e}"))?;
    send.write_all(partial_cap_bytes)
        .await
        .map_err(|e| anyhow!("send body: {e}"))?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;

    let mut status = [0u8; 1];
    recv.read_exact(&mut status)
        .await
        .map_err(|e| anyhow!("recv status: {e}"))?;
    Ok(status[0])
}

/// Send `OP_DELIVER_CAP` on a fresh stream of `conn`. Returns the
/// recipient's status byte.
pub async fn send_deliver_cap<C: Conn>(conn: &C, cap_bytes: &[u8], sig_bytes: &[u8]) -> Result<u8> {
    if cap_bytes.len() > MAX_BLOB_BYTES as usize || sig_bytes.len() > MAX_BLOB_BYTES as usize {
        return Err(anyhow!("cap or sig exceeds MAX_BLOB_BYTES"));
    }
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send.write_all(&[OP_DELIVER_CAP])
        .await
        .map_err(|e| anyhow!("send op: {e}"))?;
    send.write_all(&(cap_bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|e| anyhow!("send cap len: {e}"))?;
    send.write_all(cap_bytes)
        .await
        .map_err(|e| anyhow!("send cap body: {e}"))?;
    send.write_all(&(sig_bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|e| anyhow!("send sig len: {e}"))?;
    send.write_all(sig_bytes)
        .await
        .map_err(|e| anyhow!("send sig body: {e}"))?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;

    let mut status = [0u8; 1];
    recv.read_exact(&mut status)
        .await
        .map_err(|e| anyhow!("recv status: {e}"))?;
    Ok(status[0])
}

// ── Server-side: parse incoming streams ───────────────────────────────

/// Payload of a single incoming op, parsed but not yet acted on.
/// Byte buffers use [`anybytes::Bytes`] — the iroh stream reads
/// into a Vec via `read_exact`, but we wrap it immediately so
/// every downstream consumer (the protocol handler, the
/// `NetEvent` channel, and the policy / wire-re-send layers
/// beyond) shares one refcount instead of copying.
pub enum IncomingOp {
    Request {
        partial_cap_bytes: anybytes::Bytes,
    },
    Deliver {
        cap_bytes: anybytes::Bytes,
        sig_bytes: anybytes::Bytes,
    },
}

/// Read one operation from a freshly-accepted bi-stream. Returns
/// `Some(IncomingOp)` on a recognised op, `None` on an unknown op
/// (caller should write `STATUS_MALFORMED` + close).
pub async fn read_incoming<R: AsyncRead + Unpin>(recv: &mut R) -> Result<Option<IncomingOp>> {
    let mut op = [0u8; 1];
    recv.read_exact(&mut op)
        .await
        .map_err(|e| anyhow!("recv op: {e}"))?;
    match op[0] {
        OP_REQUEST_CAP => {
            let partial_cap_bytes = read_length_prefixed(recv).await?;
            Ok(Some(IncomingOp::Request { partial_cap_bytes }))
        }
        OP_DELIVER_CAP => {
            let cap_bytes = read_length_prefixed(recv).await?;
            let sig_bytes = read_length_prefixed(recv).await?;
            Ok(Some(IncomingOp::Deliver {
                cap_bytes,
                sig_bytes,
            }))
        }
        _ => Ok(None),
    }
}

async fn read_length_prefixed<R: AsyncRead + Unpin>(recv: &mut R) -> Result<anybytes::Bytes> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| anyhow!("recv len: {e}"))?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_BLOB_BYTES {
        return Err(anyhow!(
            "length-prefixed payload is {} bytes, exceeds MAX_BLOB_BYTES {}",
            len,
            MAX_BLOB_BYTES
        ));
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("recv body: {e}"))?;
    // Wrap the freshly-read Vec into a refcounted Bytes (zero-copy —
    // anybytes::Bytes::from_source just takes ownership of the Vec).
    Ok(anybytes::Bytes::from_source(buf))
}

/// Write a status byte and finish the stream.
pub async fn respond<W: AsyncWrite + Unpin>(send: &mut W, status: u8) -> Result<()> {
    send.write_all(&[status])
        .await
        .map_err(|e| anyhow!("send status: {e}"))?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;
    Ok(())
}

// ── One-shot endpoint helpers ─────────────────────────────────────────
//
// These wrap "spin up an iroh endpoint, dial target on the auth-
// handshake ALPN, send one op, tear down" — the shape `team approve`
// and `team request-join` need in the CLI process (which has no
// long-lived daemon to dispatch through).
//
// The endpoint config mirrors the daemon's (`host::spawn`): same
// dot-stripped relay map, same N0 (pkarr+DNS) + mDNS discovery
// providers so we can reach peers on LAN or via mainline DHT
// regardless of whether n0.computer's DNS is reachable. Outbound-only
// here — no incoming-stream router, no pkarr publish (we're not
// long-lived and don't need to be findable).

/// Build a one-shot outbound iroh Endpoint signed by `key`.
///
/// The endpoint goes online (`await ep.online()`), waits long enough
/// for the relay handshake, and is ready to call `connect`. Caller is
/// responsible for dropping it after use — endpoint drop cleans up
/// the relay subscription.
pub async fn one_shot_endpoint(key: ed25519_dalek::SigningKey) -> Result<iroh::Endpoint> {
    use iroh::Endpoint;
    use iroh::endpoint::presets;

    let secret = crate::identity::iroh_secret(&key);
    let relay_map = crate::host::dot_stripped_default_relay_map();
    // N0 preset (pkarr + DNS) + best-effort mDNS, matching
    // transport::iroh::bind. pkarr-DHT discovery dropped in the iroh 1.0
    // upgrade (removed from core).
    let ep = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .ca_tls_config(iroh::tls::CaTlsConfig::system())
        .relay_mode(iroh::RelayMode::Custom(relay_map))
        .bind()
        .await
        .map_err(|e| anyhow!("endpoint bind: {e}"))?;
    if let Ok(mdns) = iroh_mdns_address_lookup::MdnsAddressLookup::builder().build(ep.id()) {
        if let Ok(al) = ep.address_lookup() {
            al.add(mdns);
        }
    }
    ep.online().await;
    Ok(ep)
}

/// One-shot OP_REQUEST_CAP: dial `target` and send the partial cap.
/// Convenience wrapper around `one_shot_endpoint` +
/// `Endpoint::connect` + `send_request_cap`.
pub async fn one_shot_request_cap(
    key: ed25519_dalek::SigningKey,
    target: ed25519_dalek::VerifyingKey,
    partial_cap_bytes: &[u8],
) -> Result<u8> {
    use iroh_base::EndpointId;
    let ep = one_shot_endpoint(key).await?;
    let target_id =
        EndpointId::from_bytes(&target.to_bytes()).map_err(|e| anyhow!("target pubkey: {e}"))?;
    let conn = ep
        .connect(target_id, AUTH_HANDSHAKE_ALPN)
        .await
        .map_err(|e| anyhow!("connect: {e}"))?;
    let status = send_request_cap(
        &crate::transport::iroh::IrohConn(conn.clone()),
        partial_cap_bytes,
    )
    .await;
    conn.close(0u32.into(), b"ok");
    // Drop the endpoint last so the connection's relay route stays
    // alive through the close. iroh tears down a Connection's transport
    // when its parent Endpoint is dropped.
    drop(ep);
    status
}
