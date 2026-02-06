//! This module re-exports the most commonly used types and traits from the `triblespace` crate.
//! It is intended to be glob imported as `use triblespace::prelude::*;`.
//!
//! # Introduction
//!
//! The `triblespace` crate is a Rust library for working with graph data.
//! It is designed to be simple, fast, and flexible.
//!
//! # Deletion and Forgetting
//!
//! On the surface, deletion and forgetting may seem identical.
//! However, there is a subtle but crucial difference: Deletion removes
//! a statement from existence, making it no longer valid, whereas forgetting removes your knowledge
//! of it, without affecting its validity. This distinction is particularly
//! important in contexts where data is shared among multiple parties, or where
//! derived statements are based on the original data.
//! Forgetting does not propagate to other parties,
//! is reversible should the forgotten information be rediscovered,
//! and does not invalidate any derived statements or facts.
//!
//! The property that distinguishes forgetting from deletion is called _monotonicity_,
//! and it has a deep relationship with _consistency_, as laid out by the [CALM theorem](https://arxiv.org/abs/1901.01930)
//! (Consistency as Logical Monotonicity). The CALM theorem states that a distributed
//! system is consistent if and only if it is logically monotonic. This means that
//! if you want to build a consistent distributed system, you need to
//! ensure that it is logically monotonic. This is where forgetting comes
//! in: _By allowing you to forget things, but preventing you from deleting
//! things, `triblespace` allows you to build consistent distributed systems.
//!

pub mod blobschemas;
pub mod valueschemas;

pub use crate::attribute::{Attribute, AttributeUsage, AttributeUsageSource};
pub use crate::blob::Blob;
pub use crate::blob::BlobSchema;
pub use crate::blob::MemoryBlobStore;
pub use crate::blob::ToBlob;
pub use crate::blob::TryFromBlob;
pub use crate::id::fucid;
pub use crate::id::local_ids;
pub use crate::id::rngid;
pub use crate::id::ufoid;
pub use crate::id::ExclusiveId;
pub use crate::id::Id;
pub use crate::id::IdOwner;
pub use crate::id::RawId;
pub use crate::metadata::{ConstMetadata, Metadata};
pub use crate::query::find;
pub use crate::query::intersectionconstraint::and;
pub use crate::query::intersectionconstraint::IntersectionConstraint;
pub use crate::query::matches;
pub use crate::repo::pile::Pile;
pub use crate::repo::BlobStore;
pub use crate::repo::BlobStoreGet;
pub use crate::repo::BlobStoreList;
pub use crate::repo::BlobStorePut;
pub use crate::repo::BranchStore;
pub use crate::trible::Trible;
pub use crate::trible::TribleSet;
pub use crate::trible::TribleSetFingerprint;
pub use crate::value::FromValue;
pub use crate::value::ToValue;
pub use crate::value::TryFromValue;
pub use crate::value::TryToValue;
pub use crate::value::Value;
pub use crate::value::ValueSchema;
pub use anybytes::View;
// Re-export the pattern/entity procedural macros into the prelude so they can
// be imported with `use triblespace::prelude::*;` and called as `pattern!(...)`.
// After migrating away from namespace-local wrapper macros, this makes the
// new global proc-macros ergonomically available.
pub use crate::macros::attributes;
pub use crate::macros::entity;
pub use crate::macros::path;
pub use crate::macros::pattern;
pub use crate::macros::pattern_changes;
