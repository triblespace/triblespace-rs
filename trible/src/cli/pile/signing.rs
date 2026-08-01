use anyhow::Result;
use ed25519_dalek::SigningKey;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Load a stable signing identity, refusing to invent one when the operation
/// publishes or selects author-owned state.
pub(super) fn load_required_signing_key(
    path_opt: &Option<PathBuf>,
) -> Result<SigningKey, anyhow::Error> {
    let path = signing_key_path(path_opt).ok_or_else(|| {
        anyhow::anyhow!(
            "a stable signing key is required; pass --signing-key or set TRIBLES_SIGNING_KEY"
        )
    })?;
    load_key_from_file(&path)
}

fn signing_key_path(path_opt: &Option<PathBuf>) -> Option<PathBuf> {
    path_opt
        .clone()
        .or_else(|| env::var("TRIBLES_SIGNING_KEY").ok().map(PathBuf::from))
}

fn load_key_from_file(p: &Path) -> Result<SigningKey, anyhow::Error> {
    let content = fs::read_to_string(p)
        .map_err(|e| anyhow::anyhow!("failed to read signing key {}: {e}", p.display()))?;
    let hexstr = content.trim();
    if hexstr.len() != 64 || !hexstr.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!(
            "signing key file {} does not contain valid 64-char hex",
            p.display()
        );
    }
    let bytes =
        hex::decode(hexstr).map_err(|e| anyhow::anyhow!("invalid hex in signing key file: {e}"))?;
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(SigningKey::from_bytes(&arr))
}
