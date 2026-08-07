//! Canonical records, discovery, and semantic resolution for typed collections.
//!
//! Wire decoding and strict self-signature checks remain structural. Production
//! [resolution](crate::collection::resolution) admits only caller-authorized,
//! representation-validated claims. The larger generic oracle remains
//! test-only: it exercises algebraic laws rather than serving as another
//! runtime implementation.

pub mod discovery;
pub mod records;
/// Stateless semantic admission, closure, provenance, and physical-cover view.
pub mod resolution;
/// Policy-driven retention planning for admitted collection views.
pub mod retention;
/// Canonical `SimpleArchive` set-union collection kind.
pub mod simplearchive_union;

pub use discovery::*;
pub use records::*;
pub use resolution::*;
pub use retention::*;

#[cfg(test)]
mod oracle;
