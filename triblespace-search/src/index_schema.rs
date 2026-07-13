//! Typed facts shared by the range-native search index recipes.
//!
//! The stable attribute identifiers in this module were minted with
//! `trible genid` on 2026-07-13. Artifact attributes intentionally carry
//! their exact blob encodings: search manifests never erase these handles to
//! `UnknownBlob`.

use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::iu256::U256BE;
use triblespace_core::prelude::attributes;

use crate::succinct::{SuccinctBM25Blob, SuccinctHNSWBlob};

attributes! {
    /// One physical succinct BM25 artifact.
    "71775F45F379AE80B8F2A7AA40ECE0B8" as pub seg_bm25: Handle<SuccinctBM25Blob>;
    /// One physical succinct HNSW artifact.
    "54B0D283B85698E875A8A270E2570CF7" as pub seg_hnsw: Handle<SuccinctHNSWBlob>;
    /// Source attribute projected by a search recipe.
    "38FA73632BEF15C5D125AA4A8E168D84" as pub index_source_attribute: GenId;
    /// Vector dimension of an HNSW recipe.
    "45818F54828F1EAEC1FB8E34C8E290EB" as pub index_dimension: U256BE;
    /// Deterministic graph-construction seed of an HNSW recipe.
    "7E03D090721DF88BD001AA3ACCCA7256" as pub index_seed: U256BE;
}
