//! Canonical records and reference semantics for grow-only typed collections.
//!
//! The production wire codecs live in [`records`]. The bounded semantic model
//! remains test-only: it is an executable oracle for the lattice laws, not a
//! second runtime implementation.

pub mod records;

pub use records::*;

#[cfg(test)]
mod oracle;
