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
//! verify, is closed. A later `OP_AUTH` cannot replace the connection's
//! verified identity and is rejected.
//!
//! Operations:
//!   AUTH       cap_handle:32 → resp:u8                (0x00 = OK, 0x01 = REJECTED)
//!   GET_BLOB   hash:32 → len:u64 data                (u64::MAX = missing)
//!   CHILDREN   parent:32 → hash* nil                  (nil = end)
//!   (protocol is read-only — no remote writes)
//!
//! Legacy scalar-HEAD observations are gossip-driven, not ALPN-driven.
//! `CHILDREN` is an untrusted batching hint for store-relative conservative
//! references: clients bind every hint to content-verified parent bytes and
//! independently hash-verify every fetched child. Earlier protocol versions
//! also had `OP_LIST` and `OP_HEAD`; both are retired.
//! None of these legacy surfaces replicate StrongPin branch authority: there
//! is not yet a wire operation for signed branch assertions or their exact
//! `(author key, name handle)` identity.

pub const PILE_SYNC_ALPN: &[u8] = b"/triblespace/pile-sync/4";

// Operation types — first byte on each stream.
// 0x01 was OP_LIST, retired with peer-local scalar branch discovery.
pub const OP_GET_BLOB: u8 = 0x02;
pub const OP_CHILDREN: u8 = 0x03;
// 0x04 was OP_HEAD, retired alongside OP_LIST.
/// First stream on every connection. Body: cap_handle:32. Response: u8
/// status (`AUTH_OK` or `AUTH_REJECTED`). Connection state caches the
/// verified scope; subsequent ops on the same connection inherit it.
pub const OP_AUTH: u8 = 0x05;
// CAS_PUSH was removed with mutable remote branch writes. The current protocol
// is read-only; future StrongPin replication must transfer verified immutable
// assertions rather than restore scalar CAS.

/// Auth response: capability verified, all subsequent ops on this
/// connection are scope-gated by the verified cap.
pub const AUTH_OK: u8 = 0x00;
/// Auth response: capability did not verify (chain malformed, signature
/// failed, expired, scope-not-subset, fetch failed for any link, etc.).
/// The connection should be closed by the client.
pub const AUTH_REJECTED: u8 = 0x01;

/// Terminates a variable-length child-hint response. The all-zero value cannot
/// be a valid BLAKE3 content handle.
pub const NIL_HASH: RawHash = [0u8; 32];

/// Largest blob accepted from one `GET_BLOB` response.
///
/// The limit is checked against the declared wire length before allocating the
/// response buffer. It is part of the transport envelope rather than a blob
/// encoding invariant: larger local blobs remain valid, but are not replicated
/// by this protocol version.
pub const MAX_GET_BLOB_BYTES: usize = 256 * 1024 * 1024;

/// Largest number of hashes accepted from one untrusted `CHILDREN` response.
/// This independently bounds response growth even when a parent contains many
/// aligned 32-byte words.
pub const MAX_CHILD_HINTS: usize = 65_536;

pub type RawHash = [u8; 32];
pub type RawPinId = [u8; 16];

// ── Send/Recv helpers ────────────────────────────────────────────────
//
// Generic over `tokio::io::{AsyncRead, AsyncWrite}` so the same wire
// code runs over iroh QUIC streams (production) and in-memory duplex
// pipes (deterministic simulation). `SendStream::finish()` from the
// pre-seam code maps to `AsyncWriteExt::shutdown()` — iroh's QUIC
// send-stream implements `poll_shutdown` as finish.

use anyhow::{Result, anyhow};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::transport::Conn;

pub async fn send_u8<W: AsyncWrite + Unpin>(send: &mut W, v: u8) -> Result<()> {
    send.write_all(&[v]).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_hash<W: AsyncWrite + Unpin>(send: &mut W, hash: &RawHash) -> Result<()> {
    send.write_all(hash).await.map_err(|e| anyhow!("send: {e}"))
}

pub async fn send_u64_be<W: AsyncWrite + Unpin>(send: &mut W, v: u64) -> Result<()> {
    send.write_all(&v.to_be_bytes())
        .await
        .map_err(|e| anyhow!("send: {e}"))
}

pub async fn recv_u8<R: AsyncRead + Unpin>(recv: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf[0])
}

pub async fn recv_hash<R: AsyncRead + Unpin>(recv: &mut R) -> Result<RawHash> {
    let mut buf = [0u8; 32];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("recv: {e}"))?;
    Ok(buf)
}

pub async fn recv_u64_be<R: AsyncRead + Unpin>(recv: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("recv: {e}"))?;
    Ok(u64::from_be_bytes(buf))
}

// ── Single-stream operations (client side) ───────────────────────────

/// AUTH: present a capability handle. Must be the first stream opened
/// on every new connection. Returns `Ok(())` if the server accepted the
/// capability and the connection is authorised for subsequent ops.
pub async fn op_auth<C: Conn>(conn: &C, cap_handle: &RawHash) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_AUTH).await?;
    send_hash(&mut send, cap_handle).await?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;
    let resp = recv_u8(&mut recv).await?;
    match resp {
        AUTH_OK => Ok(()),
        AUTH_REJECTED => Err(anyhow!("server rejected capability")),
        other => Err(anyhow!("unknown auth response: {other:#x}")),
    }
}

/// GET_BLOB: fetch a single blob by hash.
/// Response: len:u64 + data. len=u64::MAX means missing.
/// Supports empty blobs (len=0) and rejects a declared length larger than
/// [`MAX_GET_BLOB_BYTES`] before allocating.
pub async fn op_get_blob<C: Conn>(conn: &C, hash: &RawHash) -> Result<Option<Vec<u8>>> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_GET_BLOB).await?;
    send_hash(&mut send, hash).await?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;

    recv_blob_response(&mut recv, MAX_GET_BLOB_BYTES).await
}

async fn recv_blob_response<R: AsyncRead + Unpin>(
    recv: &mut R,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let len = recv_u64_be(recv).await?;
    if len == u64::MAX {
        return Ok(None);
    }
    if len > max_bytes as u64 {
        return Err(anyhow!(
            "GET_BLOB response declares {len} bytes, exceeds limit {max_bytes}"
        ));
    }
    let mut data = vec![0u8; len as usize];
    recv.read_exact(&mut data)
        .await
        .map_err(|e| anyhow!("recv: {e}"))?;
    Ok(Some(data))
}

/// CHILDREN: get untrusted store-relative child hints for a parent blob.
/// Nil hash terminates. Callers must bind each hint to verified parent bytes
/// and hash-verify fetched children; this response is not a completeness proof.
pub async fn op_children<C: Conn>(conn: &C, parent: &RawHash) -> Result<Vec<RawHash>> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
    send_u8(&mut send, OP_CHILDREN).await?;
    send_hash(&mut send, parent).await?;
    send.shutdown().await.map_err(|e| anyhow!("finish: {e}"))?;

    recv_children_response(&mut recv, MAX_CHILD_HINTS).await
}

async fn recv_children_response<R: AsyncRead + Unpin>(
    recv: &mut R,
    max_children: usize,
) -> Result<Vec<RawHash>> {
    let mut children = Vec::new();
    loop {
        let hash = recv_hash(recv).await?;
        if hash == NIL_HASH {
            break;
        }
        if children.len() >= max_children {
            return Err(anyhow!("CHILDREN response exceeds limit {max_children}"));
        }
        children.push(hash);
    }
    Ok(children)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oversized_blob_declaration_is_rejected_before_body_read() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        tokio::spawn(async move {
            send_u64_be(&mut writer, 9).await.unwrap();
        });

        let error = recv_blob_response(&mut reader, 8).await.unwrap_err();
        assert!(error.to_string().contains("exceeds limit 8"));
    }

    #[tokio::test]
    async fn child_response_is_bounded_before_an_extra_push() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        tokio::spawn(async move {
            for byte in 1..=3 {
                send_hash(&mut writer, &[byte; 32]).await.unwrap();
            }
        });

        let error = recv_children_response(&mut reader, 2).await.unwrap_err();
        assert!(error.to_string().contains("exceeds limit 2"));
    }
}
