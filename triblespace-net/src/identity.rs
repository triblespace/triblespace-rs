//! Node identity management.
//!
//! Each node gets a persistent ed25519 identity. The key identifies
//! the node on the network and signs commits. It is NOT tied to any
//! specific pile or storage backend.
//!
//! Default location: `$TRIBLESPACE_KEY` env, or `./self.key` in the
//! current directory, or an explicit path.

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use iroh_base::SecretKey;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

/// Load or create a persistent node identity.
///
/// Resolution order:
/// 1. Explicit path (if provided) — auto-created if missing
/// 2. `TRIBLESPACE_KEY` environment variable — auto-created if missing
/// 3. `<default_dir>/self.key` — auto-created if missing
pub fn load_or_create_key(
    explicit_path: &Option<std::path::PathBuf>,
    default_dir: &Path,
) -> Result<SigningKey> {
    let key_path = resolve_key_path(explicit_path, default_dir);
    if key_path.exists() {
        return load_key_from_file(&key_path);
    }
    let key = generate_key()?;
    let hex_str = hex::encode(key.to_bytes());
    fs::write(&key_path, &hex_str)
        .map_err(|e| anyhow!("write key to {}: {e}", key_path.display()))?;
    info!(path = %key_path.display(), "generated new node key");
    Ok(key)
}

/// Load an existing persistent node identity without creating authority.
///
/// Path resolution is identical to [`load_or_create_key`], but a missing path
/// is an error. Read-only inspection should not need a key at all; commands
/// that sign policy dispositions use this boundary to avoid silently becoming
/// a new assertion author because of a typo or absent file.
pub fn load_existing_key(
    explicit_path: &Option<PathBuf>,
    default_dir: &Path,
) -> Result<SigningKey> {
    let key_path = resolve_key_path(explicit_path, default_dir);
    if !key_path.exists() {
        anyhow::bail!(
            "signing key {} does not exist; refusing to generate it",
            key_path.display()
        );
    }
    load_key_from_file(&key_path)
}

fn resolve_key_path(explicit_path: &Option<PathBuf>, default_dir: &Path) -> PathBuf {
    match explicit_path {
        Some(p) => p.clone(),
        None => match std::env::var("TRIBLESPACE_KEY") {
            Ok(s) => PathBuf::from(s),
            Err(_) => default_dir.join("self.key"),
        },
    }
}

/// Convert an ed25519 signing key to an iroh secret key.
pub fn iroh_secret(key: &SigningKey) -> SecretKey {
    SecretKey::from(key.to_bytes())
}

fn load_key_from_file(p: &Path) -> Result<SigningKey> {
    let content = fs::read_to_string(p).map_err(|e| anyhow!("read key {}: {e}", p.display()))?;
    let hexstr = content.trim();
    if hexstr.len() != 64 || !hexstr.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("key file {} is not valid 64-char hex", p.display());
    }
    let bytes = hex::decode(hexstr)?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(SigningKey::from_bytes(&arr))
}

fn generate_key() -> Result<SigningKey> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).map_err(|e| anyhow!("generate key: {e}"))?;
    Ok(SigningKey::from_bytes(&seed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_existing_key_never_creates_a_missing_explicit_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.key");

        let error = load_existing_key(&Some(path.clone()), dir.path()).unwrap_err();
        assert!(error.to_string().contains("refusing to generate it"));
        assert!(!path.exists());
    }
}
