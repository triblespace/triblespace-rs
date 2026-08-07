//! Canonical records, discovery, and reference semantics for typed collections.
//!
//! The production wire codecs live in the
//! [records module](crate::collection::records). The bounded semantic model
//! remains test-only: it is an executable oracle for the lattice laws, not a
//! second runtime implementation.

pub mod discovery;
pub mod records;
/// Canonical `SimpleArchive` set-union collection kind.
pub mod simplearchive_union;

pub use discovery::*;
pub use records::*;

#[cfg(test)]
mod oracle;
