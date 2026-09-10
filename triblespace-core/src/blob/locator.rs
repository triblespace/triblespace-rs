//! Opaque discovery names for bearer-addressed blobs.
//!
//! A content handle is also the capability to read its exact bytes. A locator
//! is only a one-way image of that handle: it can be disclosed for discovery
//! and reference summaries without disclosing the bearer capability itself.

// This is the existing network locator domain, moved here without changing
// its bytes or algorithm so producers and network consumers share one rule.
const LOCATOR_CONTEXT: &str = "triblespace.net/blob-locator/v1";

/// Derive an opaque discovery locator without disclosing the bearer handle.
///
/// Possession of this locator does not authorize an exact blob read.
pub fn blob_locator(handle: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(LOCATOR_CONTEXT);
    hasher.update(&handle);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_the_existing_network_locator_domain() {
        for handle in [[0; 32], [1; 32], [255; 32]] {
            assert_eq!(
                blob_locator(handle),
                blake3::derive_key("triblespace.net/blob-locator/v1", &handle)
            );
            assert_ne!(blob_locator(handle), handle);
        }
    }
}
