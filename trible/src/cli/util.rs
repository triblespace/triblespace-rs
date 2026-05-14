use anyhow::Result;
use triblespace::prelude::TryToInline;
use triblespace_core::value::schemas::hash::Blake3;
use triblespace_core::value::schemas::hash::Hash;

pub fn parse_blob_handle(handle: &str) -> Result<triblespace_core::value::Inline<Hash<Blake3>>> {
    handle.try_to_inline().map_err(|e| anyhow::anyhow!("{e:?}"))
}
