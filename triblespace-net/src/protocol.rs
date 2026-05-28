//! Binary wire protocol types and helpers.
//!
//! One QUIC stream per operation. The first byte identifies the operation,
//! followed by the request payload. The response follows on the same stream.
//! Stream FIN signals completion — no explicit DONE framing needed.
//!
//! Auth: the FIRST stream on every connection must be `OP_AUTH(cap_handle)`.
//! The server fetches the cap chain via the local snapshot, walks it back to
//! the configured team root, and caches the verified scope for the rest of
//! the connection. Subsequent streams are gated on that cached scope. A
//! connection whose first stream is not `OP_AUTH`, or whose cap fails to
//! verify, sees every subsequent op rejected (`AUTH_REJECTED`).
//!
//! Nil sentinels: nil id ([0u8; 16]) and nil hash ([0u8; 32]) terminate
//! sequences. P(collision) = 2^(-128) / 2^(-256). Content-addressed systems
//! already assume hash uniqueness — nil sentinels are the same assumption.
//!
//! Operations:
//!   AUTH       cap_handle:32 → resp:u8                (0x00 = OK, 0x01 = REJECTED)
//!   GET_BLOB   hash:32 → len:u64 data                (u64::MAX = missing)
//!   CHILDREN   parent:32 → hash* nil                  (nil = end)
//!   (protocol is read-only — no remote writes)
//!
//! Branch-state discovery is gossip-driven, not ALPN-driven. HEAD
//! updates flood the team topic (= team_root pubkey); subscribers
//! receive them as gossip messages and walk the reachable closure
//! via `GET_BLOB` + `CHILDREN`. Earlier protocol versions had an
//! `OP_LIST` ("enumerate this peer's branches") and `OP_HEAD`
//! ("what head does this peer have for branch X"), but those bake
//! the wrong primitive into the wire: peers don't have authoritative
//! views, the *team* does. The right discovery mechanism is "join
//! the gossip topic and let heads arrive."

pub const PILE_SYNC_ALPN: &[u8] = b"/triblespace/pile-sync/4";

// Operation types — first byte on each stream.
// 0x01 was OP_LIST, retired in favour of gossip-driven head discovery.
pub const OP_GET_BLOB: u8 = 0x02;
pub const OP_CHILDREN: u8 = 0x03;
// 0x04 was OP_HEAD, retired alongside OP_LIST.
/// First stream on every connection. Body: cap_handle:32. Response: u8
/// status (`AUTH_OK` or `AUTH_REJECTED`). Connection state caches the
/// verified scope; subsequent ops on the same connection inherit it.
pub const OP_AUTH: u8 = 0x05;
// CAS_PUSH removed: the data model is monotonic (set union), merge
// always succeeds, and each node manages its own branches locally.
// No remote writes needed — the protocol is read-only.

/// Auth response: capability verified, all subsequent ops on this
/// connection are scope-gated by the verified cap.
pub const AUTH_OK: u8 = 0x00;
/// Auth response: capability did not verify (chain malformed, signature
/// failed, expired, revoked, scope-not-subset, fetch failed for any
/// link, etc.). The connection should be closed by the client.
pub const AUTH_REJECTED: u8 = 0x01;

pub const NIL_HASH: RawHash = [0u8; 32];
pub const NIL_BRANCH_ID: RawPinId = [0u8; 16];

pub type RawHash = [u8; 32];
pub type RawPinId = [u8; 16];

// ── Send/Recv helpers ────────────────────────────────────────────────

use anyhow::{Result, anyhow};
use iroh::endpoint::{SendStream, RecvStream, Connection};

pub async fn send_u8(send: &mut SendStream, v: u8) -> Result<()> {
    send.write_all(&[v]).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_hash(send: &mut SendStream, hash: &RawHash) -> Result<()> {
    send.write_all(hash).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_branch_id(send: &mut SendStream, id: &RawPinId) -> Result<()> {
    send.write_all(id).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_u32_be(send: &mut SendStream, v: u32) -> Result<()> {
    send.write_all(&v.to_be_bytes()).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_u64_be(send: &mut SendStream, v: u64) -> Result<()> {
    send.write_all(&v.to_be_bytes()).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn recv_u8(recv: &mut RecvStream) -> Result<u8> {
    let mut buf = [0u8; 1];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf[0])
}

pub async fn recv_hash(recv: &mut RecvStream) -> Result<RawHash> {
    let mut buf = [0u8; 32];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf)
}

pub async fn recv_branch_id(recv: &mut RecvStream) -> Result<RawPinId> {
    let mut buf = [0u8; 16];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf)
}

pub async fn recv_u32_be(recv: &mut RecvStream) -> Result<u32> {
    let mut buf = [0u8; 4];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(u32::from_be_bytes(buf))
}

pub async fn recv_u64_be(recv: &mut RecvStream) -> Result<u64> {
    let mut buf = [0u8; 8];
    recv.read_exact(&mut buf).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(u64::from_be_bytes(buf))
}

// ── Single-stream operations (client side) ───────────────────────────

/// AUTH: present a capability handle. Must be the first stream opened
/// on every new connection. Returns `Ok(())` if the server accepted the
/// capability and the connection is authorised for subsequent ops.
pub async fn op_auth(conn: &Connection, cap_handle: &RawHash) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_AUTH).await?;
    send_hash(&mut send, cap_handle).await?;
    send.finish().map_err(|e| anyhow!("finish: {e}"))?;
    let resp = recv_u8(&mut recv).await?;
    match resp {
        AUTH_OK => Ok(()),
        AUTH_REJECTED => Err(anyhow!("server rejected capability")),
        other => Err(anyhow!("unknown auth response: {other:#x}")),
    }
}

/// GET_BLOB: fetch a single blob by hash.
/// Response: len:u64 + data. len=u64::MAX means missing.
/// Supports empty blobs (len=0) and blobs up to 2^64-2 bytes.
pub async fn op_get_blob(conn: &Connection, hash: &RawHash) -> Result<Option<Vec<u8>>> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_GET_BLOB).await?;
    send_hash(&mut send, hash).await?;
    send.finish().map_err(|e| anyhow!("finish: {e}"))?;

    let len = recv_u64_be(&mut recv).await?;
    if len == u64::MAX { return Ok(None); }
    let mut data = vec![0u8; len as usize];
    recv.read_exact(&mut data).await.map_err(|e| anyhow!("recv: {e}"))?;
    Ok(Some(data))
}

/// CHILDREN: get child hashes of a parent blob. Nil hash terminates.
pub async fn op_children(
    conn: &Connection,
    parent: &RawHash,
) -> Result<Vec<RawHash>> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_CHILDREN).await?;
    send_hash(&mut send, parent).await?;
    send.finish().map_err(|e| anyhow!("finish: {e}"))?;

    let mut children = Vec::new();
    loop {
        let hash = recv_hash(&mut recv).await?;
        if hash == NIL_HASH { break; }
        children.push(hash);
    }
    Ok(children)
}
